import base64
import io
import logging
import os
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

# ------------------------------------------------------------------------------
# Load .env & Logging
# ------------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fiscalflow")

# ------------------------------------------------------------------------------
# Helper: garantir que o SQL Anywhere 17 esteja no PATH (Windows)
# ------------------------------------------------------------------------------
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

_ensure_sqlany_path()

# ------------------------------------------------------------------------------
# Env: ODBC
# ------------------------------------------------------------------------------
def _get(env: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(env)
    return v if (v is not None and str(v).strip() != "") else default

SQLANY_DRIVER     = _get("SQLANY_DRIVER", "SQL Anywhere 17")
SQLANY_SERVERNAME = _get("SQLANY_SERVERNAME", "")  # opcional
SQLANY_HOST       = _get("SQLANY_HOST", "127.0.0.1")
SQLANY_PORT       = _get("SQLANY_PORT", "2638")
SQLANY_DB         = _get("SQLANY_DB") or _get("SQLANY_DBNAME", "contabil")
SQLANY_USER       = _get("SQLANY_USER") or _get("SQLANY_UID", "dba")
SQLANY_PASSWORD   = _get("SQLANY_PASSWORD") or _get("SQLANY_PWD", "sql")

# Domínio schema/tables/columns
DOM_SCHEMA    = _get("DOM_SCHEMA", "bethadba")
TB_ENTRADAS   = _get("TB_ENTRADAS", "EFENTRADAS")
TB_FORNECEDOR = _get("TB_FORNECEDOR", "EFFORNECE")

COL_CHAVE_ENT  = _get("COL_CHAVE_ENT", "CHAVE_NFE")
COL_STATUS_ENT = _get("COL_STATUS_ENT", "SITU_NFE")
COL_NUMERO_ENT = _get("COL_NUMERO_ENT", "NFIS_ENT")
COL_SERIE_ENT  = _get("COL_SERIE_ENT", "SERI_ENT")
COL_DEMI_ENT   = _get("COL_DEMI_ENT", "DEMI_ENT")
COL_DENT_ENT   = _get("COL_DENT_ENT", "DDOC_ENT")
COL_VALOR_ENT  = _get("COL_VALOR_ENT", "VLRT_ENT")

COL_COD_FOR   = _get("COL_COD_FOR", "CODI_FOR")
COL_CNPJ_FOR  = _get("COL_CNPJ_FOR", "CGCE_FOR")

MODELO_DOM_NFE = int(_get("MODELO_DOM_NFE", "36"))
MODELO_DOM_CTE = int(_get("MODELO_DOM_CTE", "56"))

# SIEG
SIEG_API_KEY      = _get("SIEG_API_KEY", "")
SIEG_BASE_URL     = _get("SIEG_BASE_URL", "https://api.sieg.com")
SIEG_REPORT_TYPE  = int(_get("SIEG_REPORT_TYPE", "2"))
SIEG_TIMEOUT      = float(_get("SIEG_TIMEOUT", "60"))

# ------------------------------------------------------------------------------
# Conexão ODBC (robusta para SERVERNAME e/ou HOST/PORT)
# ------------------------------------------------------------------------------
def odbc_connect() -> pyodbc.Connection:
    parts = []
    parts.append(f"DRIVER={{{SQL Anywhere 17}}}" if SQLANY_DRIVER == "SQL Anywhere 17"
                 else f"DRIVER={{{{}}}}".format(SQLANY_DRIVER))
    # SERVERNAME (se informado)
    if SQLANY_SERVERNAME:
        parts.append(f"SERVERNAME={SQLANY_SERVERNAME}")
    # HOST/PORT (usaremos sempre; o driver ignora se não precisar)
    if SQLANY_HOST:
        parts.append(f"HOST={SQLANY_HOST}")
    if SQLANY_PORT:
        parts.append(f"PORT={SQLANY_PORT}")
    # DB e credenciais
    parts.append(f"DATABASE={SQLANY_DB}")
    parts.append(f"UID={SQLANY_USER}")
    parts.append(f"PWD={SQLANY_PASSWORD}")
    # Opcionalmente, dá para acrescentar: "AutoStop=No;INTLTOUTF8=Yes"
    conn_str = ";".join(parts) + ";"
    log.debug("ODBC connection string: %s", conn_str)
    try:
        return pyodbc.connect(conn_str, autocommit=True)
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ODBC: {e}")

# ------------------------------------------------------------------------------
# Modelos da API
# ------------------------------------------------------------------------------
class TipoDocumento(str):
    NFE = "NFE"
    CTE = "CTE"

class Direcao(str):
    ENTRADA = "ENTRADA"
    SAIDA = "SAIDA"  # futuro

class ValidatePayload(BaseModel):
    codi_emp: int = Field(..., description="Código da empresa no Domínio")
    cnpj_empresa: str = Field(..., description="CNPJ da empresa (somente números)")
    tipo_documento: TipoDocumento
    direcao: Direcao
    data_inicio: date
    data_fim: date

    @validator("cnpj_empresa")
    def _only_digits_cnpj(cls, v):
        d = "".join(ch for ch in v if ch.isdigit())
        if len(d) != 14:
            raise ValueError("CNPJ deve ter 14 dígitos.")
        return d

    @validator("data_fim")
    def _range_ok(cls, v, values):
        di = values.get("data_inicio")
        if di and v < di:
            raise ValueError("data_fim anterior a data_inicio.")
        if di and (v - di).days > 92:
            raise ValueError("Período máximo de 3 meses.")
        return v

# ------------------------------------------------------------------------------
# Domínio: consulta entradas (NF-e e CT-e tomados)
# ------------------------------------------------------------------------------
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
    with odbc_connect() as conn:
        df = pd.read_sql(sql, conn, params=(emp, modelo, di.isoformat(), df.isoformat()))

    # Normalizações
    if "CNPJ_EMITENTE" in df.columns:
        df["CNPJ_EMITENTE"] = df["CNPJ_EMITENTE"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)

    # Normaliza status (ajuste os mapeamentos conforme seus códigos)
    if "STATUS_DOM" in df.columns:
        s = df["STATUS_DOM"].astype(str).str.upper()
        df["STATUS_DOM_NORM"] = s.map({
            "00": "AUTORIZADA",
            "02": "CANCELADA",
            "01": "DENEGADA",
        }).fillna(s)

    return df

# ------------------------------------------------------------------------------
# SIEG /api/relatorio/xml  -> DataFrame (CHAVE/STATUS/MODELO)
# ------------------------------------------------------------------------------
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
    def find(cands: List[str]) -> Optional[str]:
        for c in cands:
            for i, name in enumerate(lc):
                if c in name:
                    return cols[i]
        return None
    return {
        "chave": find(["chave", "chavenfe", "chavecte"]),
        "status": find(["sit", "situacao", "status"]),
        "modelo": find(["modelo", "xmltype", "tipo"]),
        "emissao": find(["emiss", "emissao", "dt emi", "dh emi"]),
    }

async def _fetch_sieg_month(p: SiegReportParams) -> pd.DataFrame:
    url = f"{SIEG_BASE_URL}/api/relatorio/xml"
    headers = {
        "Content-Type": "application/json",
        # Mandamos em múltiplos formatos para compatibilidade:
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
        # Algumas implantações retornam JSON {ArquivoBase64: ...}; outras retornam a string inline
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

    # Renomeia colunas relevantes
    hint = _detect_cols(df.columns.tolist())
    rename = {}
    if hint["chave"]:   rename[hint["chave"]] = "CHAVE"
    if hint["status"]:  rename[hint["status"]] = "STATUS_SIEG"
    if hint["modelo"]:  rename[hint["modelo"]] = "MODELO_SIEG"
    if hint["emissao"]: rename[hint["emissao"]] = "DATA_EMISSAO_SIEG"
    if rename:
        df = df.rename(columns=rename)

    # Normalizações
    if "CHAVE" in df.columns:
        df["CHAVE"] = df["CHAVE"].astype(str).str.replace(r"\D", "", regex=True)
        df = df[df["CHAVE"].str.len() >= 44]
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
    return pd.concat(frames, ignore_index=True)

# ------------------------------------------------------------------------------
# Cruzamento e Excel
# ------------------------------------------------------------------------------
def cross_check(df_dom: pd.DataFrame, df_sieg: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "CHAVE" not in df_dom.columns or "CHAVE" not in df_sieg.columns:
        raise HTTPException(500, "Coluna CHAVE não encontrada em uma das fontes.")
    left = df_sieg.merge(df_dom, on="CHAVE", how="left", suffixes=("_SIEG", "_DOM"))

    # faltantes = no SIEG e não no Domínio
    if "STATUS_DOM" in left.columns:
        faltantes = left[left["STATUS_DOM"].isna()].copy()
    else:
        dom_cols = [c for c in left.columns if c.endswith("_DOM")]
        faltantes = left[left[dom_cols].isna().all(axis=1)].copy()

    # divergências (status diferente)
    if "STATUS_DOM_NORM" in left.columns and "STATUS_SIEG_NORM" in left.columns:
        diverg = left[(~left["STATUS_DOM_NORM"].isna()) &
                      (~left["STATUS_SIEG_NORM"].isna()) &
                      (left["STATUS_DOM_NORM"] != left["STATUS_SIEG_NORM"])].copy()
    else:
        diverg = pd.DataFrame(columns=left.columns)

    # Seleção de colunas úteis
    keep = [c for c in [
        "CHAVE",
        "STATUS_SIEG", "STATUS_SIEG_NORM",
        "STATUS_DOM", "STATUS_DOM_NORM",
        "DATA_EMISSAO", "DATA_ENTRADA", "DATA_EMISSAO_SIEG",
        "SERIE", "NUMERO", "VALOR_TOTAL", "CNPJ_EMITENTE",
        "MODELO_SIEG", "MODELO_DOM"
    ] if c in left.columns]
    faltantes = faltantes[keep]
    diverg = diverg[keep]
    return faltantes, diverg

def build_excel(faltantes: pd.DataFrame, diverg: pd.DataFrame, meta: Dict) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (faltantes if not faltantes.empty else pd.DataFrame(columns=["CHAVE"])).to_excel(writer, index=False, sheet_name="Faltantes")
        (diverg if not diverg.empty else pd.DataFrame(columns=["CHAVE"])).to_excel(writer, index=False, sheet_name="Divergencias")
        resumo = pd.DataFrame([{
            "empresa": meta.get("empresa"),
            "cnpj_empresa": meta.get("cnpj_empresa"),
            "tipo_documento": meta.get("tipo_documento"),
            "periodo": f'{meta.get("data_inicio")} a {meta.get("data_fim")}',
            "qtde_sieg": meta.get("qtde_sieg"),
            "qtde_dom": meta.get("qtde_dom"),
            "faltantes": len(faltantes),
            "divergencias": len(diverg),
        }])
        resumo.to_excel(writer, index=False, sheet_name="Resumo")
    return bio.getvalue()

# ------------------------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------------------------
app = FastAPI(title="FiscalFlow - Validador de Notas Faltantes")

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/validator/export")
async def validator_export(payload: ValidatePayload):
    if payload.direcao != Direcao.ENTRADA:
        raise HTTPException(400, "Direção SAIDA não implementada neste MVP. Use ENTRADA.")

    # 1) Domínio
    df_dom = query_dom_entradas(payload.codi_emp, payload.tipo_documento, payload.data_inicio, payload.data_fim)
    log.info("Domínio: %s linhas", len(df_dom))

    # 2) SIEG
    df_sieg = await fetch_sieg_period(payload.cnpj_empresa, payload.tipo_documento, payload.data_inicio, payload.data_fim)
    log.info("SIEG: %s linhas", len(df_sieg))

    # 3) Cruzamento
    faltantes, diverg = cross_check(df_dom, df_sieg)

    # 4) Excel
    meta = {
        "empresa": payload.codi_emp,
        "cnpj_empresa": payload.cnpj_empresa,
        "tipo_documento": payload.tipo_documento,
        "data_inicio": payload.data_inicio.isoformat(),
        "data_fim": payload.data_fim.isoformat(),
        "qtde_sieg": len(df_sieg),
        "qtde_dom": len(df_dom),
    }
    content = build_excel(faltantes, diverg, meta)
    filename = f"validador_faltantes_{payload.tipo_documento}_{payload.codi_emp}_{payload.data_inicio}_{payload.data_fim}.xlsx"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
