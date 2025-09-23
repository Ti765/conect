import argparse
import asyncio
import base64
import io
import logging
import os
import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
APP_DIR = Path(__file__).parent

# carrega .env da raiz do projeto e o .env local desta pasta
load_dotenv()  # raiz do projeto (se existir)
load_dotenv(APP_DIR / ".env", override=False)  # .env local do app

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
# ENV HELPERS
# ==============================================================================
def _get(env: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(env)
    return v if (v is not None and str(v).strip() != "") else default

# ODBC – somente o que é realmente variável por ambiente
SQLANY_DRIVER     = _get("SQLANY_DRIVER", "SQL Anywhere 17")
SQLANY_SERVERNAME = _get("SQLANY_SERVERNAME", "")
SQLANY_HOST       = _get("SQLANY_HOST", "127.0.0.1")
SQLANY_PORT       = _get("SQLANY_PORT", "2638")
SQLANY_DB         = _get("SQLANY_DB", "contabil")
SQLANY_USER       = _get("SQLANY_USER", "dba")
SQLANY_PASSWORD   = _get("SQLANY_PASSWORD", "sql")

# SIEG
SIEG_API_KEY  = _get("SIEG_API_KEY", "")
SIEG_BASE_URL = _get("SIEG_BASE_URL", "https://api.sieg.com")
SIEG_TIMEOUT  = float(_get("SIEG_TIMEOUT", "60"))

# ==============================================================================
# Domínio – constantes FIXAS (você pediu para não depender do .env)
# ==============================================================================
DOM_SCHEMA = "bethadba"

# Entradas
TB_ENTRADAS      = "EFENTRADAS"
TB_FORNECEDOR    = "EFFORNECE"
COL_CHAVE_ENT    = "CHAVE_NFE"
COL_STATUS_ENT   = "SITU_NFE"
COL_NUMERO_ENT   = "NFIS_ENT"
COL_SERIE_ENT    = "SERI_ENT"
COL_DEMI_ENT     = "DEMI_ENT"   # emissão
COL_DENT_ENT     = "DDOC_ENT"   # entrada
COL_VALOR_ENT    = "VLRT_ENT"
COL_COD_FOR      = "CODI_FOR"
COL_CNPJ_FOR     = "CGCE_FOR"

# Saídas (NFe)
TB_SAIDAS        = "EFSAIDAS"
TB_CLIENTES      = "EFCLIENTE"
COL_CHAVE_SAI    = "CHAVE_NFE"
COL_STATUS_SAI   = "SITU_NFE"
COL_NUMERO_SAI   = "NFIS_SAI"
COL_SERIE_SAI    = "SERI_SAI"
COL_DEMI_SAI     = "DEMI_SAI"   # emissão
COL_VALOR_SAI    = "VLRT_SAI"
COL_COD_CLI      = "CODI_CLI"
COL_CNPJ_CLI     = "CGCE_CLI"

# Mapeamento de modelo no Domínio (fixo, conforme você usa)
# NFe (modelo 55) -> CODI_ESP = 36 | CT-e (57) -> CODI_ESP = 56
MODELO_DOM_NFE = 36
MODELO_DOM_CTE = 56

# Tabelas prioritárias para achar CNPJ por CODI_EMP
PRIORITY_TABLES: List[Tuple[str, str]] = [
    ("GEEMPRE", "CGCE_EMP"),
    ("GEEMPRE_VIGENCIA", "CGCE_EMP"),
    ("GEEMPRE", "CNPJ"),
    ("EFEMPRESA", "CGC_CIA"),
]
# Colunas candidatas para descoberta genérica (UPPER)
CANDIDATE_CNPJ_COLS = [
    "CGC_CIA", "CGC", "CGC_EMP", "CNPJ", "CGCE_EMP", "CNPJ_EMP"
]

# ==============================================================================
# Conexão ODBC
# ==============================================================================
def odbc_connect() -> pyodbc.Connection:
    """
    Tenta 3 estratégias (ordem importa):
      1) ENG/DBN + LINKS=TCPIP(HOST;PORT)
      2) Host=host:port + DBN=dbname
      3) ServerName=server + DBN=dbname
    (boas práticas de string de conexão do SQL Anywhere). :contentReference[oaicite:2]{index=2}
    """
    driver = "{" + (SQLANY_DRIVER or "SQL Anywhere 17") + "}"

    attempts = []

    # 1) ENG/DBN + LINKS TCPIP
    attempts.append(
        ";".join([
            f"DRIVER={driver}",
            f"ENG={SQLANY_SERVERNAME}" if SQLANY_SERVERNAME else "",
            f"DBN={SQLANY_DB}",
            f"UID={SQLANY_USER}",
            f"PWD={SQLANY_PASSWORD}",
            f"LINKS=TCPIP(HOST={SQLANY_HOST};PORT={SQLANY_PORT})",
            "AutoStop=No",
            "INTLTOUTF8=Yes",
        ])
    )

    # 2) HOST:PORT + DBN
    host_port = f"{SQLANY_HOST}:{SQLANY_PORT}" if SQLANY_HOST and SQLANY_PORT else SQLANY_HOST
    attempts.append(
        ";".join([
            f"DRIVER={driver}",
            f"Host={host_port}" if host_port else "",
            f"DBN={SQLANY_DB}",
            f"UID={SQLANY_USER}",
            f"PWD={SQLANY_PASSWORD}",
            "AutoStop=No",
            "INTLTOUTF8=Yes",
        ])
    )

    # 3) ServerName + DBN
    if SQLANY_SERVERNAME:
        attempts.append(
            ";".join([
                f"DRIVER={driver}",
                f"ServerName={SQLANY_SERVERNAME}",
                f"DBN={SQLANY_DB}",
                f"UID={SQLANY_USER}",
                f"PWD={SQLANY_PASSWORD}",
                "AutoStop=No",
                "INTLTOUTF8=Yes",
            ])
        )

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
# Pydantic v2 – ver docs de model_validator. :contentReference[oaicite:3]{index=3}

# ==============================================================================
# Util: descobrir CNPJ da empresa (TOP 1) – evita FETCH (erro que você viu)
# ==============================================================================
def get_cnpj_empresa(emp: int) -> str:
    with odbc_connect() as conn:
        cur = conn.cursor()

        # 1) Prioriza tabelas/colunas conhecidas
        for tbl, col in PRIORITY_TABLES:
            try:
                sql = f"SELECT TOP 1 {col} FROM {DOM_SCHEMA}.{tbl} WHERE CODI_EMP = ?"
                log.debug("SQL CNPJ (prior): %s", sql)
                cur.execute(sql, (emp,))
                row = cur.fetchone()
                if row and row[0]:
                    cnpj = re.sub(r"\D", "", str(row[0]))
                    if len(cnpj) == 14:
                        log.info("CNPJ %s via %s.%s: %s", emp, tbl, col, cnpj)
                        return cnpj
            except Exception:
                continue

        # 2) Catálogo genérico: encontra tabelas com CODI_EMP + coluna candidata de CNPJ
        catalog_sql = """
        SELECT u.user_name   AS owner_name,
               t.table_name  AS table_name,
               c_cnpj.column_name AS cnpj_col
          FROM sys.systab t
          JOIN sys.sysuser u        ON u.user_id = t.creator
          JOIN sys.syscolumn c_emp  ON c_emp.table_id = t.table_id AND UPPER(c_emp.column_name) = 'CODI_EMP'
          JOIN sys.syscolumn c_cnpj ON c_cnpj.table_id = t.table_id
         WHERE UPPER(u.user_name) = UPPER(?)
           AND UPPER(c_cnpj.column_name) IN ({})
        """.format(",".join("'" + c + "'" for c in CANDIDATE_CNPJ_COLS))
        # Catálogo do SQL Anywhere: systab, sysuser, syscolumn. :contentReference[oaicite:4]{index=4}

        cur.execute(catalog_sql, (DOM_SCHEMA,))
        candidates = cur.fetchall()

        for owner_name, table_name, cnpj_col in candidates:
            try:
                sql = f"SELECT TOP 1 {cnpj_col} FROM {owner_name}.{table_name} WHERE CODI_EMP = ?"
                log.debug("SQL CNPJ (catalog): %s", sql)
                cur.execute(sql, (emp,))
                row = cur.fetchone()
                if row and row[0]:
                    cnpj = re.sub(r"\D", "", str(row[0]))
                    if len(cnpj) == 14:
                        log.info("CNPJ %s via %s.%s(%s): %s", emp, owner_name, table_name, cnpj_col, cnpj)
                        return cnpj
            except Exception:
                continue

        msg = (f"Não consegui obter CNPJ para CODI_EMP={emp}. "
               f"Valide se há CGCE_EMP/CGC/CNPJ em {DOM_SCHEMA}.GEEMPRE/GEEMPRE_VIGENCIA ou em outra tabela.")
        log.error(msg)
        raise HTTPException(500, msg)

# ==============================================================================
# Queries Domínio – ENTRADAS
# ==============================================================================
def query_dom_entradas(emp: int, tipo: TipoDocumento, di: date, df: date) -> pd.DataFrame:
    modelo = MODELO_DOM_NFE if tipo == TipoDocumento.NFE else MODELO_DOM_CTE
    cols = [
        f"nf.{COL_CHAVE_ENT} AS CHAVE",
        f"nf.{COL_STATUS_ENT} AS STATUS_DOM",
        f"nf.{COL_DENT_ENT} AS DATA_ENTRADA",
        "nf.CODI_EMP",
        "nf.CODI_ESP AS MODELO_DOM",
        f"forn.{COL_CNPJ_FOR} AS CNPJ_EMITENTE",
    ]
    if COL_DEMI_ENT:   cols.append(f"nf.{COL_DEMI_ENT} AS DATA_EMISSAO")
    if COL_SERIE_ENT:  cols.append(f"nf.{COL_SERIE_ENT} AS SERIE")
    if COL_NUMERO_ENT: cols.append(f"nf.{COL_NUMERO_ENT} AS NUMERO")
    if COL_VALOR_ENT:  cols.append(f"nf.{COL_VALOR_ENT} AS VALOR_TOTAL")

    sql = f"""
      SELECT {", ".join(cols)}
        FROM {DOM_SCHEMA}.{TB_ENTRADAS} nf
        JOIN {DOM_SCHEMA}.{TB_FORNECEDOR} forn
          ON forn.CODI_EMP = nf.CODI_EMP
         AND forn.{COL_COD_FOR} = nf.{COL_COD_FOR}
       WHERE nf.CODI_EMP = ?
         AND nf.CODI_ESP = ?
         AND nf.{COL_DENT_ENT} BETWEEN DATE(?) AND DATE(?)
         AND nf.{COL_CHAVE_ENT} IS NOT NULL
    """
    log.debug("SQL ENTRADAS (%s): %s", tipo, sql)
    with odbc_connect() as conn:
        df = pd.read_sql(sql, conn, params=(emp, modelo, di.isoformat(), df.isoformat()))

    if "CNPJ_EMITENTE" in df.columns:
        df["CNPJ_EMITENTE"] = (
            df["CNPJ_EMITENTE"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
        )
    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)

    if "STATUS_DOM" in df.columns:
        s = df["STATUS_DOM"].astype(str).str.upper()
        df["STATUS_DOM_NORM"] = s.map({"00": "AUTORIZADA", "02": "CANCELADA", "01": "DENEGADA"}).fillna(s)

    log.info("Domínio ENTRADAS %s: %s linhas", tipo, len(df))
    return df

# ==============================================================================
# Queries Domínio – SAÍDAS (NFe)
# ==============================================================================
def query_dom_saidas(emp: int, tipo: TipoDocumento, di: date, df: date) -> pd.DataFrame:
    if tipo == TipoDocumento.CTE:
        log.info("CT-e SAÍDA ignorado por regra (somente tomados).")
        return pd.DataFrame(columns=["CHAVE"])

    modelo = MODELO_DOM_NFE
    cols = [
        f"sf.{COL_CHAVE_SAI} AS CHAVE",
        f"sf.{COL_STATUS_SAI} AS STATUS_DOM",
        f"sf.{COL_DEMI_SAI} AS DATA_EMISSAO",
        "sf.CODI_EMP",
        "sf.CODI_ESP AS MODELO_DOM",
        f"cli.{COL_CNPJ_CLI} AS CNPJ_DESTINATARIO",
    ]
    if COL_SERIE_SAI:  cols.append(f"sf.{COL_SERIE_SAI} AS SERIE")
    if COL_NUMERO_SAI: cols.append(f"sf.{COL_NUMERO_SAI} AS NUMERO")
    if COL_VALOR_SAI:  cols.append(f"sf.{COL_VALOR_SAI} AS VALOR_TOTAL")

    sql = f"""
      SELECT {", ".join(cols)}
        FROM {DOM_SCHEMA}.{TB_SAIDAS} sf
   LEFT JOIN {DOM_SCHEMA}.{TB_CLIENTES} cli
          ON cli.CODI_EMP = sf.CODI_EMP
         AND cli.{COL_COD_CLI} = sf.{COL_COD_CLI}
       WHERE sf.CODI_EMP = ?
         AND sf.CODI_ESP = ?
         AND sf.{COL_DEMI_SAI} BETWEEN DATE(?) AND DATE(?)
         AND sf.{COL_CHAVE_SAI} IS NOT NULL
    """
    log.debug("SQL SAIDAS (%s): %s", tipo, sql)
    with odbc_connect() as conn:
        df = pd.read_sql(sql, conn, params=(emp, modelo, di.isoformat(), df.isoformat()))

    if "CNPJ_DESTINATARIO" in df.columns:
        df["CNPJ_DESTINATARIO"] = (
            df["CNPJ_DESTINATARIO"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
        )
    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)

    if "STATUS_DOM" in df.columns:
        s = df["STATUS_DOM"].astype(str).str.upper()
        df["STATUS_DOM_NORM"] = s.map({"00": "AUTORIZADA", "02": "CANCELADA", "01": "DENEGADA"}).fillna(s)

    log.info("Domínio SAIDAS %s: %s linhas", tipo, len(df))
    return df

# ==============================================================================
# SIEG – /api/relatorio/xml (retry + escolha automática do report)
# ==============================================================================
def _sieg_report_type_for(tipo: TipoDocumento) -> int:
    # 2 = Relatório Básico (NFe) | 4 = Relatório CTe (CT-e)
    return 2 if tipo == TipoDocumento.NFE else 4

class SiegReportParams(BaseModel):
    cnpj: str
    xml_type: int    # 1=NFe, 2=CTe
    year: int
    month: int
    report_type: int

def _month_range(di: date, df: date) -> List[Tuple[int, int]]:
    y, m = di.year, di.month
    result = []
    while (y < df.year) or (y == df.year and m <= df.month):
        result.append((y, m))
        m = 1 if m == 12 else m + 1
        if m == 1:
            y += 1
    return result

def _detect_cols(cols: List[str]) -> Dict[str, Optional[str]]:
    lc = [c.lower() for c in cols]
    def find(subs: List[str]) -> Optional[str]:
        for s in subs:
            for i, name in enumerate(lc):
                if s in name:
                    return cols[i]
        return None
    return {
        "chave":      find(["chave", "chavenfe", "chavecte"]),
        "status":     find(["sit", "situacao", "status"]),
        "modelo":     find(["modelo", "xmltype", "tipo"]),
        "emissao":    find(["emiss", "emissao", "dt emi", "dh emi"]),
        "cnpj_emit":  find(["emit", "emitente", "cnpj emit", "cnpj_emit"]),
        "cnpj_dest":  find(["dest", "destinat", "cnpj dest", "cnpj_dest"]),
        "cnpj_tom":   find(["tomador", "toma", "cnpj toma", "cnpj_tom"]),
        "cnpj_rem":   find(["remet", "remetente", "cnpj rem", "cnpj_rem"]),
    }

async def _post_sieg(body: dict) -> httpx.Response:
    url = f"{SIEG_BASE_URL}/api/relatorio/xml"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        # já vi implementações aceitarem qualquer um destes:
        "x-api-key": SIEG_API_KEY,
        "api-key": SIEG_API_KEY,
        "ApiKey": SIEG_API_KEY,
        "Authorization": f"ApiKey {SIEG_API_KEY}",
    }
    async with httpx.AsyncClient(timeout=SIEG_TIMEOUT) as client:
        return await client.post(url, headers=headers, json=body)

async def _fetch_sieg_month(p: SiegReportParams) -> pd.DataFrame:
    """
    Chama /api/relatorio/xml. Em caso de 500/429/502 tenta até 3 vezes (backoff).
    Para CT-e tenta automaticamente o report_type 4 (CTe). Para NF-e usa 2.
    """
    body = {
        "Cnpj": p.cnpj,
        "TypeXmlDownloadReport": p.report_type,
        "XmlType": p.xml_type,   # 1=NFe, 2=CTe
        "Month": p.month,
        "Year": p.year,
    }

    exceptions: List[str] = []
    for attempt in (1, 2, 3):
        resp = await _post_sieg(body)
        if resp.status_code == 200:
            break
        # fallback: se CT-e com 500, tenta variar report_type (4 é o destino, mas tentamos 2→4 ou 4→2)
        if p.xml_type == 2 and attempt == 1 and p.report_type != 4:
            body["TypeXmlDownloadReport"] = 4
        elif p.xml_type == 1 and attempt == 1 and p.report_type != 2:
            body["TypeXmlDownloadReport"] = 2

        exceptions.append(f"{resp.status_code}: {resp.text[:200]}")
        await asyncio.sleep(0.7 * attempt)
    else:
        raise HTTPException(502, f"SIEG HTTP falhou: {exceptions}")

    # podem retornar JSON {ArquivoBase64: ...} ou um string base64 simples
    try:
        if "application/json" in (resp.headers.get("content-type") or ""):
            data = resp.json()
            b64 = data.get("ArquivoBase64") or data.get("Base64") or data.get("File") or data.get("Data")
        else:
            b64 = resp.text.strip().strip('"')
    except Exception:
        b64 = resp.text.strip().strip('"')

    try:
        xlsx_bytes = base64.b64decode(b64)
    except Exception as e:
        raise HTTPException(500, f"Falha ao decodificar Base64 do relatório SIEG: {e}")

    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
    ws = wb.active
    data = list(ws.values)
    if not data:
        return pd.DataFrame()
    cols = [str(c) if c is not None else "" for c in data[0]]
    df = pd.DataFrame(data[1:], columns=cols)

    hint = _detect_cols(df.columns.tolist())
    rename = {}
    if hint["chave"]:      rename[hint["chave"]] = "CHAVE"
    if hint["status"]:     rename[hint["status"]] = "STATUS_SIEG"
    if hint["modelo"]:     rename[hint["modelo"]] = "MODELO_SIEG"
    if hint["emissao"]:    rename[hint["emissao"]] = "DATA_EMISSAO_SIEG"
    if hint["cnpj_emit"]:  rename[hint["cnpj_emit"]] = "CNPJ_EMIT_SIEG"
    if hint["cnpj_dest"]:  rename[hint["cnpj_dest"]] = "CNPJ_DEST_SIEG"
    if hint["cnpj_tom"]:   rename[hint["cnpj_tom"]] = "CNPJ_TOM_SIEG"
    if hint["cnpj_rem"]:   rename[hint["cnpj_rem"]] = "CNPJ_REM_SIEG"
    if rename:
        df = df.rename(columns=rename)

    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)
        df = df[df["CHAVE"].str.len() >= 44]
    for c in ["CNPJ_EMIT_SIEG", "CNPJ_DEST_SIEG", "CNPJ_TOM_SIEG", "CNPJ_REM_SIEG"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
    if "STATUS_SIEG" in df.columns:
        s = df["STATUS_SIEG"].astype(str).str.upper()
        df["STATUS_SIEG_NORM"] = (
            s.where(~s.str.contains("CANCEL", na=False), "CANCELADA")
             .where(~s.str.contains("AUTORI", na=False), "AUTORIZADA")
        )

    return df.drop_duplicates(subset=["CHAVE"]) if "CHAVE" in df.columns else df

async def fetch_sieg_period(cnpj: str, tipo: TipoDocumento, di: date, df: date) -> pd.DataFrame:
    xml_type = 1 if tipo == TipoDocumento.NFE else 2
    report_type = _sieg_report_type_for(tipo)
    frames = []
    for (y, m) in _month_range(di, df):
        frames.append(
            await _fetch_sieg_month(
                SiegReportParams(cnpj=cnpj, xml_type=xml_type, year=y, month=m, report_type=report_type)
            )
        )
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("SIEG %s: %s linhas (bruto)", tipo, len(out))
    return out

def _filter_sieg_por_direcao(df: pd.DataFrame, cnpj: str, tipo: TipoDocumento, direcao: Direcao) -> pd.DataFrame:
    if df.empty:
        return df
    if tipo == TipoDocumento.CTE:
        # CT-e: somente tomador (recebidos)
        if direcao == Direcao.SAIDA:
            return pd.DataFrame(columns=df.columns)
        mask = False
        if "CNPJ_TOM_SIEG" in df.columns:
            mask = (df["CNPJ_TOM_SIEG"] == cnpj)
        if "CNPJ_DEST_SIEG" in df.columns:
            mask = mask | (df["CNPJ_DEST_SIEG"] == cnpj)
        return df[mask] if isinstance(mask, pd.Series) else df

    # NFe
    if direcao == Direcao.ENTRADA:
        if "CNPJ_DEST_SIEG" in df.columns:
            return df[df["CNPJ_DEST_SIEG"] == cnpj]
        return df
    else:
        if "CNPJ_EMIT_SIEG" in df.columns:
            return df[df["CNPJ_EMIT_SIEG"] == cnpj]
        return df

# ==============================================================================
# Cruzamento
# ==============================================================================
def cross_check(df_dom: pd.DataFrame, df_sieg: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df_dom.empty and df_sieg.empty:
        return pd.DataFrame(columns=["CHAVE"]), pd.DataFrame(columns=["CHAVE"])

    if "CHAVE" not in df_dom.columns or "CHAVE" not in df_sieg.columns:
        raise HTTPException(500, "Coluna CHAVE não encontrada em uma das fontes.")
    left = df_sieg.merge(df_dom, on="CHAVE", how="left", suffixes=("_SIEG", "_DOM"))

    # faltantes: existem no SIEG e não no Domínio
    if "STATUS_DOM" in left.columns:
        faltantes = left[left["STATUS_DOM"].isna()].copy()
    else:
        dom_cols = [c for c in left.columns if c.endswith("_DOM")]
        faltantes = left[left[dom_cols].isna().all(axis=1)].copy()

    # divergências de situação
    if "STATUS_DOM_NORM" in left.columns and "STATUS_SIEG_NORM" in left.columns:
        diverg = left[
            (~left["STATUS_DOM_NORM"].isna()) &
            (~left["STATUS_SIEG_NORM"].isna()) &
            (left["STATUS_DOM_NORM"] != left["STATUS_SIEG_NORM"])
        ].copy()
    else:
        diverg = pd.DataFrame(columns=left.columns)

    keep = [c for c in [
        "CHAVE",
        "STATUS_SIEG", "STATUS_SIEG_NORM",
        "STATUS_DOM", "STATUS_DOM_NORM",
        "DATA_EMISSAO", "DATA_ENTRADA", "DATA_EMISSAO_SIEG",
        "SERIE", "NUMERO", "VALOR_TOTAL",
        "CNPJ_EMITENTE", "CNPJ_DESTINATARIO",
        "MODELO_SIEG", "MODELO_DOM",
        "CNPJ_EMIT_SIEG", "CNPJ_DEST_SIEG", "CNPJ_TOM_SIEG"
    ] if c in left.columns]
    faltantes = faltantes[keep]
    diverg = diverg[keep]
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
    log.info("Empresa %s -> CNPJ %s | Período %s a %s", codi_emp, cnpj, di, df)

    sheets: Dict[str, pd.DataFrame] = {}
    totals: Dict[str, int] = {}

    for tipo in (TipoDocumento.NFE, TipoDocumento.CTE):
        df_sieg_bruto = await fetch_sieg_period(cnpj, tipo, di, df)

        # ENTRADA
        df_sieg_in = _filter_sieg_por_direcao(df_sieg_bruto, cnpj, tipo, Direcao.ENTRADA)
        df_dom_in  = query_dom_entradas(codi_emp, tipo, di, df)
        falt_in, div_in = cross_check(df_dom_in, df_sieg_in)
        sheets[f"Faltantes_Entrada_{tipo}"]    = falt_in
        sheets[f"Divergencias_Entrada_{tipo}"] = div_in
        totals[f"qt_sieg_in_{tipo}"] = len(df_sieg_in)
        totals[f"qt_dom_in_{tipo}"]  = len(df_dom_in)
        totals[f"falt_in_{tipo}"]    = len(falt_in)
        totals[f"div_in_{tipo}"]     = len(div_in)
        log.info("[%s][Entrada] SIEG=%s DOM=%s FALT=%s DIV=%s",
                 tipo, len(df_sieg_in), len(df_dom_in), len(falt_in), len(div_in))

        # SAÍDA (somente NFE)
        if tipo == TipoDocumento.NFE:
            df_sieg_out = _filter_sieg_por_direcao(df_sieg_bruto, cnpj, tipo, Direcao.SAIDA)
            df_dom_out  = query_dom_saidas(codi_emp, tipo, di, df)
            falt_out, div_out = cross_check(df_dom_out, df_sieg_out)
            sheets[f"Faltantes_Saida_{tipo}"]    = falt_out
            sheets[f"Divergencias_Saida_{tipo}"] = div_out
            totals[f"qt_sieg_out_{tipo}"] = len(df_sieg_out)
            totals[f"qt_dom_out_{tipo}"]  = len(df_dom_out)
            totals[f"falt_out_{tipo}"]    = len(falt_out)
            totals[f"div_out_{tipo}"]     = len(div_out)
            log.info("[%s][Saída] SIEG=%s DOM=%s FALT=%s DIV=%s",
                     tipo, len(df_sieg_out), len(df_dom_out), len(falt_out), len(div_out))

    resumo = {"empresa": codi_emp, "cnpj_empresa": cnpj, "periodo": f"{di} a {df}", **totals}
    content = build_excel_multi(sheets, resumo)
    filename = f"validador_faltantes_{codi_emp}_{di}_{df}.xlsx"
    return content, filename

# ==============================================================================
# FastAPI (teste local)
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
