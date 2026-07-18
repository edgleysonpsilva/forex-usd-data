# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📈 Projeto — Mercado Financeiro | nb_00_config
# MAGIC
# MAGIC ---
# MAGIC ## 🇧🇷 Português
# MAGIC
# MAGIC ### Fontes de dados (ambas alcançáveis no Databricks Free Serverless)
# MAGIC - **Snapshot atual:** ExchangeRate-API (`open.er-api.com`) — sem auth.
# MAGIC - **Histórico diário:** dataset **Federal Reserve H.10** hospedado no GitHub (datahub.io) —
# MAGIC   `raw.githubusercontent.com/datasets/exchange-rates`. Substitui a Frankfurter (bloqueada por DNS no Free).
# MAGIC
# MAGIC ### Cobertura de moedas
# MAGIC 11 moedas com histórico real no Fed H.10: majors (EUR, JPY, GBP, CHF), Ásia (CNY, INR, KRW),
# MAGIC commodity currencies (CAD, AUD) e LatAm (BRL, MXN).
# MAGIC

# COMMAND ----------

# %run por todos os demais notebooks

from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry

# ──────────────────────────────────────────────────────────────────────────
# 1) CATÁLOGO / SCHEMAS
# ──────────────────────────────────────────────────────────────────────────
CATALOG = "workspace"
PROJECT = "financeiro"
BRONZE  = f"{CATALOG}.bronze_{PROJECT}"
SILVER  = f"{CATALOG}.silver_{PROJECT}"
GOLD    = f"{CATALOG}.gold_{PROJECT}"

# ──────────────────────────────────────────────────────────────────────────
# 2) MOEDAS-ALVO (11 — todas com histórico real no Fed H.10)
# ──────────────────────────────────────────────────────────────────────────
MOEDAS_ALVO = ["BRL", "EUR", "GBP", "JPY", "MXN", "CAD", "AUD", "CHF", "CNY", "INR", "KRW"]

# ──────────────────────────────────────────────────────────────────────────
# 3) THRESHOLD do SCD2
# ──────────────────────────────────────────────────────────────────────────
THRESHOLD_CAMBIAL = 2.0  # % — variação mínima para gerar nova versão na dim_moeda_cambio que será usada na silver

# ──────────────────────────────────────────────────────────────────────────
# 4) FONTES
# ──────────────────────────────────────────────────────────────────────────
API_EXCHANGE_LATEST = "https://open.er-api.com/v6/latest/{base}"   # snapshot atual
CSV_HISTORICO = "https://raw.githubusercontent.com/datasets/exchange-rates/main/data/daily.csv"  # Fed H.10, historico

# Mapa País (dataset Fed H.10) → código ISO da moeda
PAIS_PARA_MOEDA = {
    "Brazil": "BRL", "Euro": "EUR", "United Kingdom": "GBP", "Japan": "JPY",
    "Mexico": "MXN", "Canada": "CAD", "Australia": "AUD", "Switzerland": "CHF",
    "China": "CNY", "India": "INR", "South Korea": "KRW",
}
# Fed cota estas como "USD por unidade" → inverter p/ "unidade por USD" 
INVERTER = {"EUR", "GBP", "AUD"}

# ──────────────────────────────────────────────────────────────────────────
# 4.1) JANELA HISTÓRICA — coleta desacoplada da análise
# ──────────────────────────────────────────────────────────────────────────
HIST_MODO         = "backfill"     # "backfill" = intervalo mais amplo | "incremental" = janela curta
HIST_DATA_INICIO  = "2015-01-01"   # piso do backfill (~1 década, 5 anos pré-pandemia)
HIST_JANELA_DIAS  = 45             # modo incremental: nº de pregões (overlap p/ revisões do Fed)
DIAS_CURTO_PRAZO  = 30             # janela da visão de curto prazo no Gold

# Regimes p/ análise comparativa (limites inclusivos)
REGIMES = [
    ("pre_pandemia", "2015-01-01", "2020-02-29"),
    ("pandemia",     "2020-03-01", "2021-12-31"),
    ("pos_pandemia", "2022-01-01", "9999-12-31"),
]

# ──────────────────────────────────────────────────────────────────────────
# 5) SESSÃO HTTP com retry/backoff
# ──────────────────────────────────────────────────────────────────────────
def build_session(retries: int = 3, backoff: float = 0.5) -> requests.Session:
    session = requests.Session()
    retry_cfg = Retry(total=retries, backoff_factor=backoff,
                      status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry_cfg)
    session.mount("https://", adapter); session.mount("http://", adapter)
    return session

HTTP = build_session()

# ──────────────────────────────────────────────────────────────────────────
# 6) LOG / VALIDAÇÃO
# ──────────────────────────────────────────────────────────────────────────
def log(stage: str, msg: str, count: int | None = None):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = f" | {count:,}" if count is not None else ""
    print(f"[{ts}] [{stage.upper()}] {msg}{cnt}")

def create_schemas():
    for s in (BRONZE, SILVER, GOLD):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
        log("schema", f"Schema OK: {s}")

def assert_not_empty(df, name: str): # trava o pipeline se uma tabela vier vazia
    c = df.count()
    assert c > 0, f"[ERRO] {name} está vazio!"
    log("validate", f"{name} OK", c)

create_schemas()
log("config", f"Projeto '{PROJECT}' pronto | {len(MOEDAS_ALVO)} moedas | THRESHOLD={THRESHOLD_CAMBIAL}%")
