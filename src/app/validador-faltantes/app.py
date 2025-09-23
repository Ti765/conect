import argparse
import asyncio
import base64
import io
import logging
import os
import re
import tempfile
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import httpx
import openpyxl
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator

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
# ENV HELPERS
# ==============================================================================
def _get(env: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(env)
    return v if (v is not None and str(v).strip() != "") else default

# ODBC
SQLANY_DRIVER     = _get("SQLANY_DRIVER", "SQL Anywhere 17")
SQLANY_SERVERNAME = _get("SQLANY_SERVERNAME", "")  # opcional
SQLANY_HOST       = _get("SQLANY_HOST", "127.0.0.1")
SQLANY_PORT       = _get("SQLANY_PORT", "2638")
SQLANY_DB         = _get("SQLANY_DB") or _get("SQLANY_DBNAME", "contabil")
SQLANY_USER       = _get("SQLANY_USER") or _get("SQLANY_UID", "dba")
SQLANY_PASSWORD   = _get("SQLANY_PASSWORD") or _get("SQLANY_PWD", "sql")

# Domínio – schema / TABELAS / COLUNAS
DOM_SCHEMA       = _get("DOM_SCHEMA", "bethadba")

# ENTRADAS
TB_ENTRADAS      = _get("TB_ENTRADAS", "EFENTRADAS")
TB_FORNECEDOR    = _get("TB_FORNECEDOR", "EFFORNECE")
COL_CHAVE_ENT    = _get("COL_CHAVE_ENT", "CHAVE_NFE")
COL_STATUS_ENT   = _get("COL_STATUS_ENT", "SITU_NFE")
COL_NUMERO_ENT   = _get("COL_NUMERO_ENT", "NFIS_ENT")
COL_SERIE_ENT    = _get("COL_SERIE_ENT", "SERI_ENT")
COL_DEMI_ENT     = _get("COL_DEMI_ENT", "DEMI_ENT")
COL_DENT_ENT     = _get("COL_DENT_ENT", "DDOC_ENT")   # data de entrada
COL_VALOR_ENT    = _get("COL_VALOR_ENT", "VLRT_ENT")
COL_COD_FOR      = _get("COL_COD_FOR", "CODI_FOR")
COL_CNPJ_FOR     = _get("COL_CNPJ_FOR", "CGCE_FOR")

# SAÍDAS
TB_SAIDAS        = _get("TB_SAIDAS", "EFSAIDAS")
TB_CLIENTES      = _get("TB_CLIENTES", "EFCLIENTE")
COL_CHAVE_SAI    = _get("COL_CHAVE_SAI", "CHAVE_NFE")
COL_STATUS_SAI   = _get("COL_STATUS_SAI", "SITU_NFE")
COL_NUMERO_SAI   = _get("COL_NUMERO_SAI", "NFIS_SAI")
COL_SERIE_SAI    = _get("COL_SERIE_SAI", "SERI_SAI")
COL_DEMI_SAI     = _get("COL_DEMI_SAI", "DEMI_SAI")   # data de emissão
COL_VALOR_SAI    = _get("COL_VALOR_SAI", "VLRT_SAI")
COL_COD_CLI      = _get("COL_COD_CLI", "CODI_CLI")
COL_CNPJ_CLI     = _get("COL_CNPJ_CLI", "CGCE_CLI")

# EMPRESA (para descobrir CNPJ por CODI_EMP)
TB_EMPRESA       = _get("TB_EMPRESA", "EFEMPRESA")
COL_CNPJ_EMP     = _get("COL_CNPJ_EMP", "")

# Modelos
MODELO_DOM_NFE   = int(_get("MODELO_DOM_NFE", "36"))  # NFe (55)
MODELO_DOM_CTE   = int(_get("MODELO_DOM_CTE", "56"))  # CTe (57)

# SIEG
SIEG_API_KEY     = _get("SIEG_API_KEY", "")
SIEG_BASE_URL    = _get("SIEG_BASE_URL", "https://api.sieg.com")
SIEG_REPORT_TYPE = int(_get("SIEG_REPORT_TYPE", "2"))
SIEG_TIMEOUT     = float(_get("SIEG_TIMEOUT", "60"))

# ==============================================================================
# Conexão ODBC
# ==============================================================================
def odbc_connect() -> pyodbc.Connection:
    parts = []
    # monta DRIVER={NOME}
    driver_part = "DRIVER={" + (SQLANY_DRIVER or "SQL Anywhere 17") + "}"
    parts.append(driver_part)
    if SQLANY_SERVERNAME:
        parts.append(f"SERVERNAME={SQLANY_SERVERNAME}")
    if SQLANY_HOST:
        parts.append(f"HOST={SQLANY_HOST}")
    if SQLANY_PORT:
        parts.append(f"PORT={SQLANY_PORT}")
    parts.append(f"DATABASE={SQLANY_DB}")
    parts.append(f"UID={SQLANY_USER}")
    parts.append(f"PWD={SQLANY_PASSWORD}")
    conn_str = ";".join(parts) + ";"
    log.debug("ODBC connection string: %s", conn_str)
    try:
        return pyodbc.connect(conn_str, autocommit=True)
    except pyodbc.Error as e:
        log.exception("Falha ODBC")
        raise HTTPException(status_code=500, detail=f"Erro ODBC: {e}")

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

    @validator("data_fim")
    def _range_ok(cls, v, values):
        di = values.get("data_inicio")
        if di and v < di:
            raise ValueError("data_fim anterior a data_inicio.")
        if di and (v - di).days > 92:
            raise ValueError("Período máximo de 3 meses (92 dias).")
        return v

# ==============================================================================
# Util: descobrir CNPJ da empresa
# ==============================================================================
CANDIDATE_CNPJ_COLS = ["CGC_CIA", "CGC", "CGC_EMP", "CNPJ", "CGCE_EMP", "CNPJ_EMP"]

def get_cnpj_empresa(emp: int) -> str:
    with odbc_connect() as conn:
        cur = conn.cursor()
        cols_try = [COL_CNPJ_EMP] if COL_CNPJ_EMP else CANDIDATE_CNPJ_COLS
        last_err = None
        for col in cols_try:
            try:
                sql = f"SELECT {col} FROM {DOM_SCHEMA}.{TB_EMPRESA} WHERE CODI_EMP = ?"
                log.debug("SQL CNPJ empresa: %s", sql)
                cur.execute(sql, (emp,))
                row = cur.fetchone()
                if row and row[0]:
                    cnpj = re.sub(r"\D", "", str(row[0]))
                    if len(cnpj) == 14:
                        log.info("CNPJ empresa %s obtido por coluna %s: %s", emp, col, cnpj)
                        return cnpj
            except Exception as e:
                last_err = e
                continue
        msg = f"Não foi possível obter o CNPJ para CODI_EMP={emp}. Ajuste TB_EMPRESA/COL_CNPJ_EMP no .env"
        if last_err:
            msg += f" (último erro: {last_err})"
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
        df["CNPJ_EMITENTE"] = df["CNPJ_EMITENTE"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
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
        df["CNPJ_DESTINATARIO"] = df["CNPJ_DESTINATARIO"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)

    if "STATUS_DOM" in df.columns:
        s = df["STATUS_DOM"].astype(str).str.upper()
        df["STATUS_DOM_NORM"] = s.map({"00": "AUTORIZADA", "02": "CANCELADA", "01": "DENEGADA"}).fillna(s)

    log.info("Domínio SAIDAS %s: %s linhas", tipo, len(df))
    return df

# ==============================================================================
# SIEG – /api/relatorio/xml
# ==============================================================================
class SiegReportParams(BaseModel):
    cnpj: str
    xml_type: int    # 1=NFe, 2=CTe
    year: int
    month: int

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

async def _fetch_sieg_month(p: SiegReportParams) -> pd.DataFrame:
    url = f"{SIEG_BASE_URL}/api/relatorio/xml"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": SIEG_API_KEY,
        "api-key": SIEG_API_KEY,
        "ApiKey": SIEG_API_KEY,
        "Authorization": f"ApiKey {SIEG_API_KEY}",
    }
    body = {
        "Cnpj": p.cnpj,
        "TypeXmlDownloadReport": SIEG_REPORT_TYPE,
        "XmlType": p.xml_type,   # 1=NFe, 2=CTe
        "Month": p.month,
        "Year": p.year,
    }
    async with httpx.AsyncClient(timeout=SIEG_TIMEOUT) as client:
        resp = await client.post(url, headers=headers, json=body)
        if resp.status_code != 200:
            raise HTTPException(502, f"SIEG HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            if "application/json" in (resp.headers.get("content-type") or ""):
                data = resp.json()
                b64 = data.get("ArquivoBase64") or data.get("Base64") or data.get("File") or data.get("Data")
            else:
                b64 = resp.text.strip().strip('"')
        except Exception:
            b64 = resp.text.strip().strip('"')

    xlsx_bytes = base64.b64decode(b64)
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
    frames = []
    for (y, m) in _month_range(di, df):
        frames.append(await _fetch_sieg_month(SiegReportParams(cnpj=cnpj, xml_type=xml_type, year=y, month=m)))
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("SIEG %s: %s linhas (bruto)", tipo, len(out))
    return out

def _filter_sieg_por_direcao(df: pd.DataFrame, cnpj: str, tipo: TipoDocumento, direcao: Direcao) -> pd.DataFrame:
    if df.empty:
        return df
    if tipo == TipoDocumento.CTE:
        if direcao == Direcao.SAIDA:
            return pd.DataFrame(columns=df.columns)
        mask = False
        if "CNPJ_TOM_SIEG" in df.columns:
            mask = (df["CNPJ_TOM_SIEG"] == cnpj)
        if "CNPJ_DEST_SIEG" in df.columns:
            mask = mask | (df["CNPJ_DEST_SIEG"] == cnpj)
        return df[mask] if isinstance(mask, pd.Series) else df

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

    if "STATUS_DOM" in left.columns:
        faltantes = left[left["STATUS_DOM"].isna()].copy()
    else:
        dom_cols = [c for c in left.columns if c.endswith("_DOM")]
        faltantes = left[left[dom_cols].isna().all(axis=1)].copy()

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
