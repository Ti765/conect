#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classificador de Fornecedores
----------------------------
Versão: 2025-07-01  (Windows + pyodbc + ZIP)
"""

from __future__ import annotations

# ---------------------------------------------------------------------- #
# Imports                                                                #
# ---------------------------------------------------------------------- #
import argparse
import os
import re
import shutil
import sys
import tempfile
import zipfile
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import uuid                              # ← para nome aleatório do zip

import pandas as pd
import pyodbc                            # driver ODBC
from bs4 import BeautifulSoup
from dotenv import load_dotenv           # lê .env

load_dotenv()                            # carrega variáveis de ambiente do arquivo .env

# ---------------------------------------------------------------------- #
# 0. Configuração de logging                                             #
# ---------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
)

# ---------------------------------------------------------------------- #
# 1. Argumentos                                                          #
# ---------------------------------------------------------------------- #
def _parse_date(txt: str) -> str:
    """Converte 'YYYY-MM-DD' ou 'DD/MM/AAAA' em 'YYYY-MM-DD'."""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        f"Data '{txt}' inválida – use YYYY-MM-DD ou DD/MM/AAAA."
    )


def _args():
    p = argparse.ArgumentParser(description="Classificador de Fornecedores")
    p.add_argument("--input-dir", required=True, help="Diretório de XML/ZIP")
    p.add_argument("--empresa", required=True, type=int)
    p.add_argument("--data-ini", required=True, type=_parse_date)
    p.add_argument("--data-fim", required=True, type=_parse_date)
    return p.parse_args()


args = _args()
IN_DIR = Path(args.input_dir).expanduser().resolve()
if not IN_DIR.exists():
    sys.exit(f"Pasta de entrada inexistente: {IN_DIR}")

OUT_ROOT = IN_DIR.parent / "ARQUIVOS CLASSIFICADOS"
OUT_ROOT.mkdir(parents=True, exist_ok=True)

EMPRESA = args.empresa
DATA_INI, DATA_FIM = args.data_ini, args.data_fim

# ---------------------------------------------------------------------- #
# 2. Variáveis de ambiente SQL Anywhere                                  #
# ---------------------------------------------------------------------- #
logging.info(">>> DEBUG SQLANY_BASE = %r", os.getenv("SQLANY_BASE"))
logging.info(">>> DEBUG PATH        = %r", os.getenv("PATH"))

HOST   = os.getenv("SQLANY_HOST", "172.16.20.10")
PORT   = int(os.getenv("SQLANY_PORT", "2638"))
DBNAME = os.getenv("SQLANY_DB", "contabil")
UID    = os.getenv("SQLANY_USER", "BI")
PWD    = os.getenv("SQLANY_PASSWORD", "4431610")

MAX_PATH = 250          # limite Windows
TRUNC    = 20

TMP_XML = Path(tempfile.mkdtemp(prefix="xml_classif_"))

# ---------------------------------------------------------------------- #
# 3. Filtro CFOP                                                         #
# ---------------------------------------------------------------------- #
GROUP_CFOP = {
    "COMBUSTÍVEIS E LUBRIFICANTES": ["5653", "5656", "6653", "6656", "7667"],
    "CONSERTOS": ["5915", "5916", "6915", "6916"],
    "DEMONSTRAÇÕES": ["5912", "5913", "6912", "6913"],
    "DEVOLUÇÕES": [
        "5201","5202","5208","5209","5210","5410","5411","5412","5413",
        "5553","5555","5556","5918","5919","6201","6202","6208","6209",
        "6210","6410","6411","6412","6413","6553","6555","6556","6918",
        "6919","7201","7202","7210","7211","7212"
    ],
    "ENERGIA ELÉTRICA": [
        "5153","5207","5251","5252","5253","5254","5255","5256",
        "5257","5258","6153","6207","6251","6252","6253","6254",
        "6255","6256","6257","6258","7207","7251"
    ],
    "SERVIÇOS": [
        "5205","5301","5302","5303","5304","5305","5306","5307","5932",
        "5933","6205","6301","6302","6303","6304","6305","6306","6307",
        "6932","6933","7205","7301"
    ],
    "TRANSPORTE": [
        "5206","5351","5352","5353","5354","5355","5356","5357","5359",
        "5360","6206","6351","6352","6353","6354","6355","6356","6357",
        "6359","6360","7206","7358"
    ],
    "TRANSFERÊNCIAS": [
        "5151","5152","5155","5156","5408","5409","5552","5557",
        "6151","6152","6155","6156","6408","6409","6552","6557"
    ],
    "BONIFICAÇÕES E BRINDES": ["5910", "6910"],
    "REMESSAS": ["5920", "6920"],
    "OUTRAS": ["5601","5602","5605","5929","5949","6929","6949","7949"],
}
ALL_CFOPS = {cf for lst in GROUP_CFOP.values() for cf in lst}


def _extrair_cfops(xml_path: Path) -> list[str]:
    try:
        soup = BeautifulSoup(xml_path.read_text(errors="ignore"), "xml")
        return [t.text.strip() for t in soup.find_all("CFOP")]
    except Exception as e:
        logging.warning("Falha lendo %s: %s", xml_path.name, e)
        return []


def _classificar_cfops(cfops: set[str]) -> tuple[str, str]:
    if any(c not in ALL_CFOPS for c in cfops):
        return "PASSAR PARA CLASSIFICADOR", "Regra 1"
    grupos = {g for g, l in GROUP_CFOP.items() if cfops & set(l)}
    if len(cfops) == 1:
        return (next(iter(grupos)) if grupos else "OUTRAS", "Regra 3")
    if len(grupos) == 1:
        g = next(iter(grupos))
        return (g if g != "OUTRAS" else "OUTRAS", "Regra 2-b")
    return "OUTRAS", "Regra 2"


def _copiar(src: Path, destino: str, regra: str, log: list[dict]):
    if destino == "PASSAR PARA CLASSIFICADOR":
        shutil.copy2(src, TMP_XML / src.name)
    else:
        dst_dir = OUT_ROOT / destino
        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst_dir / src.name)
    log.append({"arquivo": src.name, "destino": destino, "regra": regra})


def _processar_zip(z: Path, log: list[dict]):
    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(z) as zf:
            zf.extractall(td)
        for xml in Path(td).rglob("*.xml"):
            cf = set(_extrair_cfops(xml))
            dest, reg = _classificar_cfops(cf)
            _copiar(xml, dest, reg, log)


def _processar_xml(x: Path, log: list[dict]):
    cf = set(_extrair_cfops(x))
    dest, reg = _classificar_cfops(cf)
    _copiar(x, dest, reg, log)


def filtro_cfop() -> pd.DataFrame:
    inicio = datetime.now()
    zips = list(IN_DIR.glob("*.zip"))
    xmls = list(IN_DIR.glob("*.xml"))
    if not zips and not xmls:
        raise FileNotFoundError(f"Nenhum .zip ou .xml em {IN_DIR}")
    log: list[dict] = []
    for z in zips:
        _processar_zip(z, log)
    for x in xmls:
        _processar_xml(x, log)
    df = pd.DataFrame(log)
    logging.info("Filtro: %d arquivos em %s", len(df), datetime.now() - inicio)
    return df


# ---------------------------------------------------------------------- #
# 4. Conexão e consultas                                                 #
# ---------------------------------------------------------------------- #
def _connect():
    """Conecta via ODBC (driver “SQL Anywhere 17”)."""
    conn_str = (
        "DRIVER={SQL Anywhere 17};"
        f"SERVERNAME={os.getenv('SQLANY_SERVERNAME', 'srvcontabil')};"
        f"HOST={HOST};PORT={PORT};"
        f"DATABASE={DBNAME};UID={UID};PWD={PWD};"
    )
    logging.debug("ODBC conn string: %s", conn_str)
    return pyodbc.connect(conn_str, autocommit=True)


def consulta_periodo(emp: int, ini: str, fim: str) -> pd.DataFrame:
    with _connect() as conn:
        sql = """
        SELECT DISTINCT
               acu.CODI_ACU, acu.NOME_ACU,
               forn.CODI_FOR  AS CODIGO_FORNECEDOR,
               forn.NOME_FOR  AS NOME_FORNECEDOR,
               forn.CGCE_FOR
          FROM bethadba.EFENTRADAS nf
          JOIN bethadba.EFACUMULADOR acu
            ON acu.CODI_EMP = nf.CODI_EMP
           AND acu.CODI_ACU = nf.CODI_ACU
          JOIN bethadba.EFACUMULADOR_VIGENCIA vig
            ON vig.CODI_EMP = nf.CODI_EMP
           AND vig.CODI_ACU = nf.CODI_ACU
           AND vig.LANCAR_SOMENTE_ENTRADA = 'S'
           AND vig.IDEV_ACU = 'N'
          JOIN bethadba.EFFORNECE forn
            ON forn.CODI_EMP = nf.CODI_EMP
           AND forn.CODI_FOR = nf.CODI_FOR
         WHERE nf.CODI_EMP = ?
           AND nf.DDOC_ENT BETWEEN DATE(?) AND DATE(?)
           AND nf.CODI_ESP = 36;
        """
        df = pd.read_sql(sql, conn, params=(emp, ini, fim))
    df["CGCE_FOR"] = (
        df["CGCE_FOR"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
    )
    return df


def consulta_fornecedores(emp: int) -> pd.DataFrame:
    with _connect() as conn:
        sql = """
        SELECT CODI_FOR, NOME_FOR, CGCE_FOR
          FROM bethadba.EFFORNECE
         WHERE CODI_EMP = ?;
        """
        df = pd.read_sql(sql, conn, params=(emp,))
    df["CGCE_FOR"] = (
        df["CGCE_FOR"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(14)
    )
    return df


# ---------------------------------------------------------------------- #
# 5. Classificador                                                       #
# ---------------------------------------------------------------------- #
def clean_name(txt: str, limit: int = 80) -> str:
    trans = str.maketrans(
        "ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇç",
        "AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCc",
    )
    txt = (txt or "").translate(trans)
    txt = re.sub(r"[^A-Za-z0-9 _\-.]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip().rstrip(" .")
    return txt[:limit]


def trunc(txt: str, n: int = TRUNC) -> str:
    return clean_name(txt)[:n]


def extrair_emitente(xml_path: Path) -> tuple[str, str]:
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
    try:
        root = ET.parse(xml_path).getroot()
        emit = root.find(".//nfe:emit", ns)
        cnpj = re.sub(r"\D", "", emit.find("nfe:CNPJ", ns).text).zfill(14)
        nome = clean_name(emit.find("nfe:xNome", ns).text)
        return cnpj, nome
    except Exception as e:
        logging.warning("Falha XML %s: %s", xml_path.name, e)
        return None, None


class Classificador:
    def __init__(self, base: Path, out: Path,
                 df_per: pd.DataFrame, df_full: pd.DataFrame):
        self.base, self.out = base, out
        self.temp = Path(tempfile.mkdtemp(prefix="xmlclas_"))
        self.map_per = self._build_map(df_per)
        self.map_full = {
            r.CGCE_FOR: (str(r.CODI_FOR), clean_name(r.NOME_FOR))
            for _, r in df_full.iterrows()
        }
        self.multi: list[dict] = []

    # --- helpers ------------------------------------------------------- #
    @staticmethod
    def _build_map(df: pd.DataFrame) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for _, r in df.iterrows():
            out.setdefault(r.CGCE_FOR, []).append({
                "acu": str(r.CODI_ACU),
                "nacu": clean_name(r.NOME_ACU),
                "cod": str(r.CODIGO_FORNECEDOR),
                "nfor": clean_name(r.NOME_FORNECEDOR),
            })
        return out

    def _copy_in(self):
        for p in self.base.rglob("*"):
            if p.suffix.lower() == ".zip":
                with zipfile.ZipFile(p) as z:
                    z.extractall(self.temp)
            elif p.suffix.lower() == ".xml":
                shutil.copy2(p, self.temp)
        self.xmls = list({p.resolve() for p in self.temp.rglob("*.xml")})
        logging.info("Classificador: XMLs prontos: %d", len(self.xmls))

    def _ensure_dir(self, path: Path) -> Path:
        fixed = Path(*[seg.rstrip(" .") for seg in path.parts])
        fixed.mkdir(parents=True, exist_ok=True)
        return fixed

    def _safe_dst(self, dst: Path) -> Path:
        return dst if len(str(dst)) < MAX_PATH else dst.parent.parent / trunc(dst.parent.name) / dst.name

    def _move(self, src: Path, dst: Path):
        dst2 = self._safe_dst(dst)
        dest_dir = self._ensure_dir(dst2.parent)
        try:
            shutil.move(src, dest_dir / dst2.name)
        except FileNotFoundError:
            shutil.copy2(src, dest_dir / dst2.name)
            src.unlink(missing_ok=True)

    # --- run ----------------------------------------------------------- #
    def run(self):
        self._copy_in()
        dirF = self.out / "Fornecedores"
        dirM = self.out / "MultiGrupo"
        dirS = self.out / "SemGrupo"
        for d in (dirF, dirM, dirS):
            d.mkdir(parents=True, exist_ok=True)
        sem: dict[str, str] = {}

        for xml in self.xmls:
            cnpj, nome = extrair_emitente(xml)
            if not cnpj:
                continue
            recs = self.map_per.get(cnpj, [])
            if not recs:
                self._move(xml, dirS / cnpj / xml.name)
                sem[cnpj] = nome
                continue
            grupos = {(r["acu"], r["nacu"]) for r in recs}
            cod, nfor = recs[0]["cod"], recs[0]["nfor"]
            if len(grupos) == 1:
                acu, nacu = grupos.pop()
                self._move(xml, dirF / f"{acu}_{nacu}" / f"{cod}_{nfor}" / xml.name)
            else:
                self._move(xml, dirM / f"{cod}_{nfor}" / xml.name)
                self.multi.append({
                    "CNPJ": cnpj,
                    "Fornecedor": f"{cod}_{nfor}",
                    "Acumuladores": ", ".join(sorted({r["acu"] for r in recs})),
                })

        # renomeia pastas de SemGrupo com info de fornecedor conhecido
        for cnpj, nome_xml in sem.items():
            old = dirS / cnpj
            cod, nome_full = self.map_full.get(cnpj, ("0000", nome_xml))
            new_base = f"{cod}_{nome_full}"
            new = dirS / clean_name(new_base)
            i = 1
            while new.exists():
                new = dirS / f"{clean_name(new_base)}_{i}"
                i += 1
            try:
                old.rename(new)
            except Exception as e:
                logging.warning("Rename falhou %s → %s: %s", old.name, new.name, e)

        if self.multi:
            pd.DataFrame(self.multi).drop_duplicates().to_excel(
                self.out / "MultiGrupo_Summary.xlsx",
                index=False,
                engine="openpyxl",
            )
        shutil.rmtree(self.temp, ignore_errors=True)
        logging.info("Classificador: Processo finalizado")


# ---------------------------------------------------------------------- #
# 6. Main                                                                #
# ---------------------------------------------------------------------- #
def main():
    logging.info("Empresa %s | Período %s → %s", EMPRESA, DATA_INI, DATA_FIM)
    try:
        df = filtro_cfop()
    except FileNotFoundError as e:
        logging.error(e)
        sys.exit(1)

    if df.empty:
        logging.info("Nenhum XML encontrado – encerrando.")
        return

    if (df["destino"] == "PASSAR PARA CLASSIFICADOR").any():
        df_p = consulta_periodo(EMPRESA, DATA_INI, DATA_FIM)
        df_f = consulta_fornecedores(EMPRESA)
        Classificador(TMP_XML, OUT_ROOT, df_p, df_f).run()

    # ------------------------------------------------------------------ #
    # 7. Gera ZIP com o resultado                                        #
    # ------------------------------------------------------------------ #
    zip_name = f"classificados_{uuid.uuid4().hex[:8]}"
    zip_path = shutil.make_archive(
        base_name=str(OUT_ROOT.parent / zip_name),
        format="zip",
        root_dir=OUT_ROOT
    )
    # avisa o caller (route.ts) onde está o arquivo
    print(f"ZIP_OK:{zip_path}", flush=True)

    shutil.rmtree(TMP_XML, ignore_errors=True)
    print(f"Concluido. Resultados em: {OUT_ROOT}")


if __name__ == "__main__":
    main()
