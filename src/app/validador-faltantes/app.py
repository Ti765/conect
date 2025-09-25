import argparse
import asyncio
import base64
import io
import logging
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import httpx
import openpyxl
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator

# ==============================================================================
# Setup (.env + LOG)
# ==============================================================================
load_dotenv()
LOG_LEVEL = os.getenv("FISCALFLOW_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("fiscalflow.validator")

# ==============================================================================
# SQL Anywhere PATH (Windows)
# ==============================================================================
def _ensure_sqlany_path():
    base = os.getenv("SQLANY_BASE")
    if not base:
        return
    candidates = [
        os.path.join(base, "bin64"),
        os.path.join(base, "bin32"),
        os.path.join(base, "Bin64"),
        os.path.join(base, "Bin32"),
    ]
    current_path = os.environ.get("PATH", "")
    prepend = [p for p in candidates if os.path.isdir(p) and p not in current_path]
    if prepend:
        os.environ["PATH"] = ";".join(prepend + [current_path])
        log.debug("PATH atualizado com SQL Anywhere: %s", os.environ["PATH"])

_ensure_sqlany_path()

# ==============================================================================
# Helpers
# ==============================================================================
def _get(env: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(env)
    return v if (v is not None and str(v).strip() != "") else default

def _clean_api_key(k: Optional[str]) -> str:
    return unquote((k or "").strip().strip('"').strip("'"))

def _digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", str(s or ""))

def _iso_date_only(s: str) -> str:
    return (s or "")[:10]

def _upper_list(xs: List[str]) -> List[str]:
    return [x.upper() for x in xs]

# ==============================================================================
# ODBC / Domínio – conexão
# ==============================================================================
SQLANY_DRIVER     = _get("SQLANY_DRIVER", "SQL Anywhere 17")
SQLANY_SERVERNAME = _get("SQLANY_SERVERNAME", "")           # ex.: srvcontabil
SQLANY_HOST       = _get("SQLANY_HOST", "127.0.0.1")        # ex.: 172.16.20.10
SQLANY_PORT       = _get("SQLANY_PORT", "2638")
SQLANY_DB         = _get("SQLANY_DB") or _get("SQLANY_DBNAME", "Contabil")
SQLANY_USER       = _get("SQLANY_USER") or _get("SQLANY_UID", "dba")
SQLANY_PASSWORD   = _get("SQLANY_PASSWORD") or _get("SQLANY_PWD", "sql")

DOM_SCHEMA        = _get("DOM_SCHEMA", "bethadba")

# Tabelas padrão (ajustadas ao que você usa nos BIs)
TB_ENTRADAS       = _get("TB_ENTRADAS",  "EFENTRADAS")
TB_SAIDAS         = _get("TB_SAIDAS",    "EFSAIDAS")
TB_FORNECEDOR     = _get("TB_FORNECEDOR","EFFORNECE")
TB_CLIENTES       = _get("TB_CLIENTES",  "EFCLIENTES")  # << plural

# Modelos Domínio (internos)
MODELO_DOM_NFE    = int(_get("MODELO_DOM_NFE", "36"))  # NFe (55)
MODELO_DOM_CTE    = int(_get("MODELO_DOM_CTE", "56"))  # CTe (57)

# SIEG
SIEG_API_KEY      = _clean_api_key(_get("SIEG_API_KEY", ""))
SIEG_BASE_URL     = _get("SIEG_BASE_URL", "https://api.sieg.com")
SIEG_TIMEOUT      = float(_get("SIEG_TIMEOUT", "120"))   # respostas grandes
SIEG_TAKE         = int(_get("SIEG_TAKE", "50"))
SIEG_RATE_SLEEP   = float(_get("SIEG_RATE_SLEEP", "3.2"))  # ~20 req/min

# ==============================================================================
# Conexão ODBC
# ==============================================================================
def odbc_connect() -> pyodbc.Connection:
    """
    Estratégias em ordem (SQL Anywhere):
      1) ENG/DBN + LINKS=TCPIP(HOST=;PORT=)
      2) Host=host:port + DBN
      3) ServerName=...
    """
    driver = "{" + (SQLANY_DRIVER or "SQL Anywhere 17") + "}"
    attempts = []

    attempts.append(";".join([
        f"DRIVER={driver}",
        f"ENG={SQLANY_SERVERNAME}" if SQLANY_SERVERNAME else "",
        f"DBN={SQLANY_DB}",
        f"UID={SQLANY_USER}",
        f"PWD={SQLANY_PASSWORD}",
        f"LINKS=TCPIP(HOST={SQLANY_HOST};PORT={SQLANY_PORT})",
        "AutoStop=No",
        "INTLTOUTF8=Yes",
    ]))
    host_port = f"{SQLANY_HOST}:{SQLANY_PORT}" if SQLANY_HOST and SQLANY_PORT else SQLANY_HOST
    attempts.append(";".join([
        f"DRIVER={driver}",
        f"Host={host_port}" if host_port else "",
        f"DBN={SQLANY_DB}",
        f"UID={SQLANY_USER}",
        f"PWD={SQLANY_PASSWORD}",
        "AutoStop=No",
        "INTLTOUTF8=Yes",
    ]))
    if SQLANY_SERVERNAME:
        attempts.append(";".join([
            f"DRIVER={driver}",
            f"ServerName={SQLANY_SERVERNAME}",
            f"DBN={SQLANY_DB}",
            f"UID={SQLANY_USER}",
            f"PWD={SQLANY_PASSWORD}",
            "AutoStop=No",
            "INTLTOUTF8=Yes",
        ]))

    last_err = None
    for i, raw in enumerate(attempts, 1):
        conn_str = ";".join([p for p in raw.split(";") if p]) + ";"
        log.debug("ODBC try %s: %s", i, conn_str)
        try:
            return pyodbc.connect(conn_str, autocommit=True)
        except pyodbc.Error as e:
            last_err = e
            log.warning("ODBC tentativa %s falhou: %s", i, e)

    raise HTTPException(status_code=500, detail=f"Erro ODBC após tentativas: {last_err}")

# ==============================================================================
# Descoberta de colunas reais (catálogo)
# ==============================================================================
def _list_columns(conn: pyodbc.Connection, owner: str, table: str) -> List[str]:
    sql = """
      SELECT c.column_name
        FROM sys.systab t
        JOIN sys.sysuser u   ON u.user_id = t.creator
        JOIN sys.syscolumn c ON c.table_id = t.table_id
       WHERE UPPER(u.user_name) = UPPER(?)
         AND UPPER(t.table_name) = UPPER(?)
    """
    rows = conn.cursor().execute(sql, (owner, table)).fetchall()
    return [r[0] for r in rows]

def _pick(columns: List[str], candidates: List[str]) -> Optional[str]:
    cols_up = _upper_list(columns)
    for cand in candidates:
        try:
            try_idx = cols_up.index(cand.upper())
            return columns[try_idx]
        except ValueError:
            continue
    return None

class DomColMap(BaseModel):
    tabela: str
    chave: Optional[str]
    emissao: Optional[str]
    numero: Optional[str]
    serie: Optional[str]
    status: Optional[str]
    valor: Optional[str]
    modelo: str = "CODI_ESP"
    cnpj_join_col: Optional[str] = None  # CGCE_FOR ou CGCE_CLI
    join_dim: Optional[str] = None       # EFFORNECE / EFCLIENTES
    join_key_nf: Optional[str] = None    # CODI_FOR / CODI_CLI
    join_key_dim: Optional[str] = None   # CODI_FOR / CODI_CLI

def discover_dom_columns() -> Tuple[DomColMap, DomColMap]:
    """
    Descobre, por catálogo, as colunas que realmente existem nas suas tabelas EFENTRADAS/EFSAIDAS.
    """
    with odbc_connect() as conn:
        cols_in  = _list_columns(conn, DOM_SCHEMA, TB_ENTRADAS)
        cols_out = _list_columns(conn, DOM_SCHEMA, TB_SAIDAS)

    # Entradas
    ent = DomColMap(
        tabela=TB_ENTRADAS,
        chave   = _pick(cols_in,  ["CHAVE_NFE_ENT","CHAVE_ENT","CHAVE_NFE","CHAVE_NFSE_ENT"]),
        emissao = _pick(cols_in,  ["DDOC_ENT","DEMI_ENT","DENT_ENT","DATA_EMISSAO","DATA_ENTRADA"]),
        numero  = _pick(cols_in,  ["NUME_ENT","NFIS_ENT","NUM_DOCUMENTO"]),
        serie   = _pick(cols_in,  ["SERI_ENT","SERIE_ENT","SUB_SERIE_ENT"]),
        status  = _pick(cols_in,  ["SITU_ENT","SITUACAO_ENT","SITU_NFE"]),
        valor   = _pick(cols_in,  ["VCON_ENT","VLRT_ENT","VALOR_TOTAL"]),
        cnpj_join_col="CGCE_FOR",
        join_dim=TB_FORNECEDOR,
        join_key_nf="CODI_FOR",
        join_key_dim="CODI_FOR",
    )

    # Saídas
    sai = DomColMap(
        tabela=TB_SAIDAS,
        chave   = _pick(cols_out, ["CHAVE_NFE_SAI","CHAVE_SAI","CHAVE_NFE"]),
        emissao = _pick(cols_out, ["DDOC_SAI","DSAI_SAI","DATA_SAIDA","DATA_EMISSAO"]),
        numero  = _pick(cols_out, ["NUME_SAI","NFIS_SAI","NUM_DOCUMENTO"]),
        serie   = _pick(cols_out, ["SERI_SAI","SERIE_SAI","SUB_SERIE_SAI","SERI_IDX_SAI"]),
        status  = _pick(cols_out, ["SITU_SAI","SITUACAO_SAI","SITU_NFE"]),
        valor   = _pick(cols_out, ["VCON_SAI","VLRT_SAI","VALOR_TOTAL"]),
        cnpj_join_col="CGCE_CLI",
        join_dim=TB_CLIENTES,
        join_key_nf="CODI_CLI",
        join_key_dim="CODI_CLI",
    )

    # Sanidade mínima:
    if not ent.chave:
        raise HTTPException(500, f"Não encontrei coluna de CHAVE em {DOM_SCHEMA}.{TB_ENTRADAS}.")
    if not ent.emissao:
        raise HTTPException(500, f"Não encontrei coluna de EMISSÃO em {DOM_SCHEMA}.{TB_ENTRADAS}.")
    if not sai.chave:
        raise HTTPException(500, f"Não encontrei coluna de CHAVE em {DOM_SCHEMA}.{TB_SAIDAS}.")
    if not sai.emissao:
        raise HTTPException(500, f"Não encontrei coluna de EMISSÃO em {DOM_SCHEMA}.{TB_SAIDAS}.")

    log.info("Mapeamento Entradas: chave=%s emissao=%s numero=%s serie=%s status=%s valor=%s",
             ent.chave, ent.emissao, ent.numero, ent.serie, ent.status, ent.valor)
    log.info("Mapeamento Saídas:   chave=%s emissao=%s numero=%s serie=%s status=%s valor=%s",
             sai.chave, sai.emissao, sai.numero, sai.serie, sai.status, sai.valor)
    return ent, sai

# ==============================================================================
# Modelos / Enums
# ==============================================================================
class TipoDocumento(str):
    NFE = "NFE"
    CTE = "CTE"

class Direcao(str):
    ENTRADA = "ENTRADA"
    SAIDA = "SAIDA"

class MinimalPayload(BaseModel):
    codi_emp: int = Field(..., description="Código da empresa no Domínio")
    data_inicio: date
    data_fim: date
    @model_validator(mode="after")
    def _check_dates(self):
        if self.data_fim < self.data_inicio:
            raise ValueError("data_fim anterior a data_inicio.")
        if (self.data_fim - self.data_inicio).days > 92:
            raise ValueError("Período máximo de 3 meses (92 dias).")
        return self

# ==============================================================================
# Descobrir CNPJ da Empresa
# ==============================================================================
def get_cnpj_empresa(emp: int) -> str:
    candidates = [
        ( "GEEMPRE",           "CGCE_EMP" ),
        ( "GEEMPRE_VIGENCIA",  "CGCE_EMP" ),
        ( "GEEMPRE",           "CNPJ"     ),
        ( "GEEMPRE_VIGENCIA",  "CNPJ"     ),
    ]
    with odbc_connect() as conn:
        cur = conn.cursor()
        for tbl, col in candidates:
            try:
                sql = f"SELECT TOP 1 {col} FROM {DOM_SCHEMA}.{tbl} WHERE CODI_EMP = ?"
                cur.execute(sql, (emp,))
                row = cur.fetchone()
                if row and row[0]:
                    cnpj = _digits(row[0])
                    if len(cnpj) == 14:
                        log.info("CNPJ %s via %s.%s: %s", emp, tbl, col, cnpj)
                        return cnpj
            except Exception:
                continue
    raise HTTPException(500, f"Não consegui obter CNPJ para CODI_EMP={emp} no schema {DOM_SCHEMA}.")

# ==============================================================================
# Queries Domínio (usando **sempre** a data de EMISSÃO)
# ==============================================================================
def _normalize_status_series(s: pd.Series) -> pd.Series:
    s2 = s.astype(str).str.upper()
    s2 = s2.where(~s2.str.contains("CANC", na=False), "CANCELADA")
    s2 = s2.where(~s2.str.contains("DENEG", na=False), "DENEGADA")
    s2 = s2.replace({"00": "AUTORIZADA", "0": "AUTORIZADA", "A": "AUTORIZADA"})
    return s2

def query_dom_entradas(emp: int, tipo: TipoDocumento, di: date, df: date, ent: DomColMap) -> pd.DataFrame:
    modelo = MODELO_DOM_NFE if tipo == TipoDocumento.NFE else MODELO_DOM_CTE

    cols = [
        f"nf.{ent.chave} AS CHAVE",
        f"nf.{ent.emissao} AS DATA_EMISSAO",
        "nf.CODI_EMP",
        "nf.CODI_ESP AS MODELO_DOM",
    ]
    if ent.status: cols.append(f"nf.{ent.status} AS STATUS_DOM")
    if ent.numero: cols.append(f"nf.{ent.numero} AS NUMERO")
    if ent.serie:  cols.append(f"nf.{ent.serie}  AS SERIE")
    if ent.valor:  cols.append(f"nf.{ent.valor}  AS VALOR_TOTAL")
    if ent.cnpj_join_col:
        cols.append(f"forn.{ent.cnpj_join_col} AS CNPJ_EMITENTE")

    sql = f"""
      SELECT {", ".join(cols)}
        FROM {DOM_SCHEMA}.{ent.tabela} nf
   LEFT JOIN {DOM_SCHEMA}.{TB_FORNECEDOR} forn
          ON forn.CODI_EMP = nf.CODI_EMP
         AND forn.{ent.join_key_dim} = nf.{ent.join_key_nf}
       WHERE nf.CODI_EMP = ?
         AND nf.CODI_ESP = ?
         AND nf.{ent.emissao} BETWEEN DATE(?) AND DATE(?)
         AND nf.{ent.chave} IS NOT NULL
    """
    log.debug("SQL ENTRADAS (%s): %s", tipo, sql)
    with odbc_connect() as conn:
        df_out = pd.read_sql(sql, conn, params=(emp, modelo, di.isoformat(), df.isoformat()))

    if "CHAVE" in df_out.columns:
        df_out["CHAVE"] = df_out["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)
    if ent.status and "STATUS_DOM" in df_out.columns:
        df_out["STATUS_DOM_NORM"] = _normalize_status_series(df_out["STATUS_DOM"])
    if "CNPJ_EMITENTE" in df_out.columns:
        df_out["CNPJ_EMITENTE"] = df_out["CNPJ_EMITENTE"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)

    log.info("Domínio ENTRADAS %s: %s linhas", tipo, len(df_out))
    return df_out

def query_dom_saidas(emp: int, tipo: TipoDocumento, di: date, df: date, sai: DomColMap) -> pd.DataFrame:
    if tipo == TipoDocumento.CTE:
        log.info("CT-e SAÍDA ignorado por regra (somente tomados).")
        return pd.DataFrame(columns=["CHAVE"])

    cols = [
        f"sf.{sai.chave} AS CHAVE",
        f"sf.{sai.emissao} AS DATA_EMISSAO",
        "sf.CODI_EMP",
        "sf.CODI_ESP AS MODELO_DOM",
        f"cli.{sai.cnpj_join_col} AS CNPJ_DESTINATARIO",
    ]
    if sai.status: cols.append(f"sf.{sai.status} AS STATUS_DOM")
    if sai.numero: cols.append(f"sf.{sai.numero} AS NUMERO")
    if sai.serie:  cols.append(f"sf.{sai.serie}  AS SERIE")
    if sai.valor:  cols.append(f"sf.{sai.valor}  AS VALOR_TOTAL")

    sql = f"""
      SELECT {", ".join(cols)}
        FROM {DOM_SCHEMA}.{sai.tabela} sf
   LEFT JOIN {DOM_SCHEMA}.{TB_CLIENTES} cli
          ON cli.CODI_EMP = sf.CODI_EMP
         AND cli.{sai.join_key_dim} = sf.{sai.join_key_nf}
       WHERE sf.CODI_EMP = ?
         AND sf.CODI_ESP = ?
         AND sf.{sai.emissao} BETWEEN DATE(?) AND DATE(?)
         AND sf.{sai.chave} IS NOT NULL
    """
    log.debug("SQL SAIDAS (%s): %s", tipo, sql)
    with odbc_connect() as conn:
        df_out = pd.read_sql(sql, conn, params=(emp, MODELO_DOM_NFE, di.isoformat(), df.isoformat()))

    if "CHAVE" in df_out.columns:
        df_out["CHAVE"] = df_out["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)
    if sai.status and "STATUS_DOM" in df_out.columns:
        df_out["STATUS_DOM_NORM"] = _normalize_status_series(df_out["STATUS_DOM"])
    if "CNPJ_DESTINATARIO" in df_out.columns:
        df_out["CNPJ_DESTINATARIO"] = df_out["CNPJ_DESTINATARIO"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)

    log.info("Domínio SAIDAS %s: %s linhas", tipo, len(df_out))
    return df_out

# ==============================================================================
# SIEG – BaixarXmls (download em lote base64)
# ==============================================================================
def _sieg_url(path: str) -> str:
    if not SIEG_API_KEY:
        raise HTTPException(500, "SIEG_API_KEY não configurada no .env")
    base = SIEG_BASE_URL.rstrip("/")
    return f"{base}/{path.lstrip('/')}?api_key={quote(SIEG_API_KEY, safe='')}"

def _xml_localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag

def _find_text_any(root: ET.Element, names: List[str]) -> Optional[str]:
    setn = set(names)
    for el in root.iter():
        if _xml_localname(el.tag) in setn and (el.text is not None):
            return el.text.strip()
    return None

def _find_first(root: ET.Element, names: List[str]) -> Optional[ET.Element]:
    setn = set(names)
    for el in root.iter():
        if _xml_localname(el.tag) in setn:
            return el
    return None

def _attr_ci(el: ET.Element, *cands: str) -> Optional[str]:
    for k in cands:
        for a in (k, k.lower(), k.upper(), k.capitalize()):
            v = el.attrib.get(a)
            if v:
                return v
    return None

def _parse_nfe(xml_bytes: bytes) -> Dict:
    d: Dict = {}
    root = ET.fromstring(xml_bytes)
    inf = _find_first(root, ["infNFe"])
    if inf is not None:
        idv = _attr_ci(inf, "Id", "ID")
        if idv and idv.upper().startswith("NFE"):
            d["CHAVE"] = _digits(idv[3:])
    if not d.get("CHAVE"):
        ch = _find_text_any(root, ["chNFe"])
        if ch:
            d["CHAVE"] = _digits(ch)

    dh = _find_text_any(root, ["dhEmi"]) or _find_text_any(root, ["dEmi"])
    if dh:
        d["DATA_EMISSAO_SIEG"] = _iso_date_only(dh)

    emit = _find_first(root, ["emit"])
    if emit is not None:
        cnpj_emit = _find_text_any(emit, ["CNPJ"])
        if cnpj_emit:
            d["CNPJ_EMIT_SIEG"] = _digits(cnpj_emit)
    dest = _find_first(root, ["dest"])
    if dest is not None:
        cnpj_dest = _find_text_any(dest, ["CNPJ"])
        if cnpj_dest:
            d["CNPJ_DEST_SIEG"] = _digits(cnpj_dest)

    d["MODELO_SIEG"] = 55
    return d

def _parse_cte(xml_bytes: bytes) -> Dict:
    d: Dict = {}
    root = ET.fromstring(xml_bytes)

    inf = _find_first(root, ["infCte","infCTe"])
    if inf is not None:
        idv = _attr_ci(inf, "Id", "ID")
        if idv and (idv.upper().startswith("CTE") or idv.upper().startswith("CT")):
            d["CHAVE"] = _digits(idv[3:])
    if not d.get("CHAVE"):
        ch = _find_text_any(root, ["chCTe"])
        if ch:
            d["CHAVE"] = _digits(ch)

    dh = _find_text_any(root, ["dhEmi"]) or _find_text_any(root, ["dEmi"])
    if dh:
        d["DATA_EMISSAO_SIEG"] = _iso_date_only(dh)

    for tag, out in [("emit","CNPJ_EMIT_SIEG"),("dest","CNPJ_DEST_SIEG"),
                     ("rem","CNPJ_REM_SIEG"),("toma3","CNPJ_TOM_SIEG"),("toma4","CNPJ_TOM_SIEG")]:
        el = _find_first(root, [tag])
        if el is not None:
            c = _find_text_any(el, ["CNPJ"])
            if c:
                d[out] = _digits(c)

    d["MODELO_SIEG"] = 57
    return d

def _make_filters(cnpj: str, tipo: TipoDocumento, di: date, df: date, direcao: Direcao) -> List[Dict]:
    base = {
        "XmlType": 1 if tipo == TipoDocumento.NFE else 2,
        "Take": SIEG_TAKE,
        "Skip": 0,
        "DataEmissaoInicio": f"{di.isoformat()}T00:00:00",
        "DataEmissaoFim": f"{df.isoformat()}T23:59:59",
    }
    if tipo == TipoDocumento.NFE:
        return [{**base, "CnpjDest": cnpj}] if direcao == Direcao.ENTRADA else [{**base, "CnpjEmit": cnpj}]
    else:
        return [{**base, "CnpjTom": cnpj}] if direcao == Direcao.ENTRADA else []

async def _sieg_download_pages(filters: Dict) -> List[bytes]:
    url = _sieg_url("BaixarXmls")
    xml_bytes_list: List[bytes] = []
    take = int(filters.get("Take") or 50)
    skip = int(filters.get("Skip") or 0)
    headers = {"Content-Type": "application/json","Accept": "application/json"}

    async with httpx.AsyncClient(timeout=SIEG_TIMEOUT) as client:
        while True:
            payload = {**filters, "Take": take, "Skip": skip}
            resp = await client.post(url, headers=headers, json=payload)
            body = (resp.text or "").strip()

            if resp.status_code == 200:
                if body.startswith('"') and body.endswith('"'):
                    body = body[1:-1]
                if not body:
                    break

                parts = [p for p in body.split(",") if p.strip()]
                got = 0
                for p in parts:
                    b64 = p.strip().strip('"')
                    try:
                        xml_bytes = base64.b64decode(b64, validate=False)
                        if xml_bytes and xml_bytes.lstrip().startswith(b"<"):
                            xml_bytes_list.append(xml_bytes); got += 1
                    except Exception as e:
                        log.warning("Base64 inválido ignorado (skip=%s): %s", skip, e)

                log.info("SIEG page Take=%s Skip=%s => %s XML(s)", take, skip, got)
                if got < take:
                    break
                skip += take
                await asyncio.sleep(SIEG_RATE_SLEEP)
                continue

            if resp.status_code == 400 and body in ("[]",""):
                log.info("SIEG retornou 400 + [] (sem resultados) – fim.")
                break
            if resp.status_code in (401,403):
                raise HTTPException(resp.status_code, f"Não Autenticado/Autorizado no SIEG: {body[:500]}")
            raise HTTPException(502, f"SIEG HTTP {resp.status_code}: {body[:500]}")
    return xml_bytes_list

async def fetch_sieg_lote(cnpj: str, tipo: TipoDocumento, di: date, df: date, direcao: Direcao) -> pd.DataFrame:
    filters_list = _make_filters(cnpj, tipo, di, df, direcao)
    if not filters_list:
        return pd.DataFrame(columns=["CHAVE"])
    rows: List[Dict] = []
    for flt in filters_list:
        xmls = await _sieg_download_pages(flt)
        log.info("SIEG %s/%s: %s XML(s) brutos", tipo, direcao, len(xmls))
        for xb in xmls:
            try:
                info = _parse_nfe(xb) if tipo == TipoDocumento.NFE else _parse_cte(xb)
                if not info.get("CHAVE"): continue
                info["CHAVE"] = re.sub(r"\D", "", info["CHAVE"])
                for k in ["CNPJ_EMIT_SIEG","CNPJ_DEST_SIEG","CNPJ_TOM_SIEG","CNPJ_REM_SIEG"]:
                    if k in info and info[k]:
                        info[k] = _digits(info[k]).zfill(14)
                if info.get("DATA_EMISSAO_SIEG"):
                    try: _ = date.fromisoformat(info["DATA_EMISSAO_SIEG"])
                    except Exception: info["DATA_EMISSAO_SIEG"] = _iso_date_only(info["DATA_EMISSAO_SIEG"])
                rows.append(info)
            except Exception as e:
                log.warning("Falha ao parsear XML (ignorado): %s", e)

    if not rows:
        return pd.DataFrame(columns=["CHAVE"])

    df = pd.DataFrame(rows)
    df = df[df["CHAVE"].str.len() >= 44].drop_duplicates(subset=["CHAVE"]).copy()
    df["DIRECAO"] = direcao
    return df

# ==============================================================================
# Cruzamento
# ==============================================================================
def cross_check(df_dom: pd.DataFrame, df_sieg: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df_dom.empty and df_sieg.empty:
        return pd.DataFrame(columns=["CHAVE"]), pd.DataFrame(columns=["CHAVE"])
    if "CHAVE" not in df_dom.columns or "CHAVE" not in df_sieg.columns:
        raise HTTPException(500, "Coluna CHAVE não encontrada em uma das fontes.")

    left = df_sieg.merge(df_dom, on="CHAVE", how="left", suffixes=("_SIEG","_DOM"))

    dom_cols = [c for c in left.columns if c.endswith("_DOM")] or ["CHAVE"]
    faltantes = left[left[dom_cols].isna().all(axis=1)].copy()

    if "STATUS_DOM" in left.columns:
        diverg = left[left["STATUS_DOM"].notna()].copy()
    else:
        diverg = pd.DataFrame(columns=left.columns)

    keep = [c for c in [
        "CHAVE",
        "DATA_EMISSAO_SIEG", "DATA_EMISSAO",
        "STATUS_DOM","STATUS_DOM_NORM",
        "SERIE","NUMERO","VALOR_TOTAL",
        "CNPJ_EMITENTE","CNPJ_DESTINATARIO",
        "MODELO_SIEG","MODELO_DOM","DIRECAO"
    ] if c in left.columns]

    faltantes = faltantes[keep] if not faltantes.empty else pd.DataFrame(columns=keep)
    diverg   = diverg[keep]   if not diverg.empty   else pd.DataFrame(columns=keep)
    return faltantes, diverg

# ==============================================================================
# Excel
# ==============================================================================
def build_excel_multi(sheets: Dict[str, pd.DataFrame], resumo: Dict) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, df in sheets.items():
            (df if not df.empty else pd.DataFrame(columns=["CHAVE"])).to_excel(
                writer, index=False, sheet_name=name[:31]
            )
        pd.DataFrame([resumo]).to_excel(writer, index=False, sheet_name="Resumo")
    return bio.getvalue()

# ==============================================================================
# Orquestração
# ==============================================================================
async def generate_all(codi_emp: int, di: date, df: date) -> Tuple[bytes, str]:
    cnpj = get_cnpj_empresa(codi_emp)
    ent_map, sai_map = discover_dom_columns()
    log.info("Empresa %s -> CNPJ %s | Período %s a %s", codi_emp, cnpj, di, df)

    sheets: Dict[str, pd.DataFrame] = {}
    totals: Dict[str, int] = {}

    # NFe - Entradas
    df_sieg_in_nfe = await fetch_sieg_lote(cnpj, TipoDocumento.NFE, di, df, Direcao.ENTRADA)
    df_dom_in_nfe  = query_dom_entradas(codi_emp, TipoDocumento.NFE, di, df, ent_map)
    falt_in_nfe, div_in_nfe = cross_check(df_dom_in_nfe, df_sieg_in_nfe)
    sheets["Faltantes_Entrada_NFE"]    = falt_in_nfe
    sheets["Divergencias_Entrada_NFE"] = div_in_nfe
    totals["qt_sieg_in_NFE"] = len(df_sieg_in_nfe)
    totals["qt_dom_in_NFE"]  = len(df_dom_in_nfe)
    totals["falt_in_NFE"]    = len(falt_in_nfe)
    totals["div_in_NFE"]     = len(div_in_nfe)

    # NFe - Saídas
    df_sieg_out_nfe = await fetch_sieg_lote(cnpj, TipoDocumento.NFE, di, df, Direcao.SAIDA)
    df_dom_out_nfe  = query_dom_saidas(codi_emp, TipoDocumento.NFE, di, df, sai_map)
    falt_out_nfe, div_out_nfe = cross_check(df_dom_out_nfe, df_sieg_out_nfe)
    sheets["Faltantes_Saida_NFE"]    = falt_out_nfe
    sheets["Divergencias_Saida_NFE"] = div_out_nfe
    totals["qt_sieg_out_NFE"] = len(df_sieg_out_nfe)
    totals["qt_dom_out_NFE"]  = len(df_dom_out_nfe)
    totals["falt_out_NFE"]    = len(falt_out_nfe)
    totals["div_out_NFE"]     = len(div_out_nfe)

    # CT-e (somente tomados / entradas)
    df_sieg_in_cte = await fetch_sieg_lote(cnpj, TipoDocumento.CTE, di, df, Direcao.ENTRADA)
    try:
        df_dom_in_cte  = query_dom_entradas(codi_emp, TipoDocumento.CTE, di, df, ent_map)
    except Exception as e:
        log.warning("CT-e no Domínio não consultado (provável ausência de CHAVE/EMISSÃO no mapeamento): %s", e)
        df_dom_in_cte = pd.DataFrame(columns=["CHAVE"])
    falt_in_cte, div_in_cte = cross_check(df_dom_in_cte, df_sieg_in_cte)
    sheets["Faltantes_Entrada_CTE"]    = falt_in_cte
    sheets["Divergencias_Entrada_CTE"] = div_in_cte
    totals["qt_sieg_in_CTE"] = len(df_sieg_in_cte)
    totals["qt_dom_in_CTE"]  = len(df_dom_in_cte)
    totals["falt_in_CTE"]    = len(falt_in_cte)
    totals["div_in_CTE"]     = len(div_in_cte)

    resumo = {"empresa": codi_emp, "cnpj_empresa": cnpj, "periodo": f"{di} a {df}", **totals}
    content = build_excel_multi(sheets, resumo)
    filename = f"validador_faltantes_{codi_emp}_{di}_{df}.xlsx"
    return content, filename

# ==============================================================================
# FastAPI
# ==============================================================================
app = FastAPI(title="FiscalFlow - Validador de Notas Faltantes")

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/validator/export")
async def validator_export(payload: MinimalPayload):
    content, filename = await generate_all(payload.codi_emp, payload.data_inicio, payload.data_fim)
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ==============================================================================
# CLI
# ==============================================================================
def _parse_args():
    p = argparse.ArgumentParser(description="Validador de Notas Faltantes (Entradas/Saídas)")
    p.add_argument("--empresa", type=int, required=True, help="CODI_EMP")
    p.add_argument("--data-ini", type=str, required=True, help="AAAA-MM-DD")
    p.add_argument("--data-fim", type=str, required=True, help="AAAA-MM-DD")
    p.add_argument("--out", type=str, default="", help="Caminho do XLSX (opcional)")
    return p.parse_args()

def _to_date(s: str) -> date:
    return date.fromisoformat(s)

def _save_temp_excel(content: bytes, preferred_name: str) -> str:
    tmpdir = tempfile.gettempdir()
    path = os.path.join(tmpdir, preferred_name)
    with open(path, "wb") as f:
        f.write(content)
    return path

if __name__ == "__main__":
    args = _parse_args()
    di = _to_date(args.data_ini)
    df_ = _to_date(args.data_fim)
    try:
        content, fname = asyncio.run(generate_all(args.empresa, di, df_))
        out_path = args.out or _save_temp_excel(content, fname)
        print(f"XLSX_OK:{out_path}", flush=True)
    except Exception:
        log.exception("Falha na execução CLI")
        raise
