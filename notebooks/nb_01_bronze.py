# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Projeto 09 — Financeiro | nb_01_bronze
# MAGIC ### Ingestão de câmbio no Databricks Free Serverless — DADOS REAIS
# MAGIC
# MAGIC - **taxas_atual_raw**  ← `open.er-api.com` (snapshot atual, bases USD e EUR)  
# MAGIC - **taxas_historico_raw** ← Fed H.10 via GitHub (histórico diário real)   
# MAGIC
# MAGIC Cobertura do histórico: 11 moedas reais (BRL, EUR, GBP, JPY, MXN, CAD, AUD, CHF, CNY, INR, KRW).

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

from pyspark.sql import functions as F, Row
from datetime import date
from uuid import uuid4
import csv, io

run_id = str(uuid4())
log("bronze", f"run_id={run_id}")

# ──────────────────────────────────────────────────────────────────────────
# FONTE 1 — SNAPSHOT ATUAL (open.er-api.com), multi-base USD + EUR 
# ──────────────────────────────────────────────────────────────────────────
def fetch_taxa_atual(base: str) -> dict:
    url = API_EXCHANGE_LATEST.format(base=base)
    try:
        resp = HTTP.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log("bronze", f"Falha ao buscar snapshot base={base}: {e}")
        return {}

def coletar_snapshot(bases=("USD", "EUR")) -> list:
    rows, hoje = [], date.today().isoformat()
    for base in bases:
        rates = fetch_taxa_atual(base).get("rates", {})
        for moeda in MOEDAS_ALVO:
            if moeda in rates and moeda != base:
                rows.append(Row(moeda_codigo=moeda, taxa_usd=float(rates[moeda]),
                                 data_ref=hoje, base_moeda=base))
    return rows

rows_atual = coletar_snapshot()
if rows_atual:
    df_atual = (
        spark.createDataFrame(rows_atual)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_api", F.lit("open.er-api.com"))
        .withColumn("_pipeline_run", F.lit(run_id))
    )
    (df_atual.write.format("delta").mode("overwrite")           # — partition base_moeda
        .option("overwriteSchema", "true")
        .partitionBy("base_moeda")
        .saveAsTable(f"{BRONZE}.taxas_atual_raw"))
    log("bronze", "taxas_atual_raw salvo", df_atual.count())
else:
    log("bronze", "⚠️ Snapshot vazio — abortando")
    dbutils.notebook.exit("sem_snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FONTE 2 — Histórico REAL (Federal Reserve H.10 via GitHub / datahub.io)

# COMMAND ----------

def baixar_historico_datahub(dias: int = 30) -> tuple:
    """Lê o CSV diário oficial (Fed H.10). Retorna (linhas, paises_nao_mapeados)."""
    resp = HTTP.get(CSV_HISTORICO, timeout=60)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))

    registros, paises_vistos, nao_mapeados = [], set(), set()
    for row in reader:
        pais = row.get("Country", "")
        paises_vistos.add(pais)
        moeda = PAIS_PARA_MOEDA.get(pais)
        if not moeda:
            nao_mapeados.add(pais)
            continue
        val = row.get("Exchange rate", "")
        try:
            taxa = float(val)
        except (ValueError, TypeError):
            continue
        if taxa <= 0:
            continue
        if moeda in INVERTER:            # normaliza p/ "unidade por USD"
            taxa = 1.0 / taxa
        registros.append((row["Date"], moeda, round(taxa, 6)))

    if not registros:
        return [], nao_mapeados

    datas_disp = sorted({r[0] for r in registros})
    corte = set(datas_disp[-dias:])      # últimas N datas de negociação (não dias de calendário)
    log("bronze", f"Fed H.10: dataset até {datas_disp[-1]} | janela {sorted(corte)[0]}..{sorted(corte)[-1]}")
    linhas = [Row(data_ref=d, moeda_codigo=m, taxa_usd=t, base_moeda="USD")
              for (d, m, t) in registros if d in corte]
    # países que estão no nosso mapa mas não apareceram (divergência de nome)
    esperados_ausentes = set(PAIS_PARA_MOEDA) - (paises_vistos & set(PAIS_PARA_MOEDA))
    if esperados_ausentes:
        log("bronze", f"⚠️ Países esperados NÃO encontrados no CSV: {esperados_ausentes}")
    return linhas, nao_mapeados

rows_hist, _ = baixar_historico_datahub(dias=30)

bronze_hist = f"{BRONZE}.taxas_historico_raw"
if rows_hist:
    df_hist = (
        spark.createDataFrame(rows_hist)
        .withColumn("ano_mes", F.date_format("data_ref", "yyyy-MM"))    #  coluna ano_mes
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_run", F.lit(run_id))
        .withColumn("_origem_dados", F.lit("REAL_fedH10_datahub"))
    )
    (df_hist.write.format("delta").mode("overwrite")                    # partition ano_mes
        .option("overwriteSchema", "true")
        .partitionBy("ano_mes")
        .saveAsTable(bronze_hist))
    log("bronze", "taxas_historico_raw salvo (REAL)", df_hist.count())
else:
    log("bronze", "⚠️ Nenhuma linha do histórico Fed H.10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verificações do Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- snapshot atual
# MAGIC SELECT base_moeda, COUNT(*) AS moedas
# MAGIC FROM workspace.bronze_financeiro.taxas_atual_raw
# MAGIC GROUP BY base_moeda;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- volume do histórico (~11 moedas × 30 dias ≈ 330 linhas)
# MAGIC SELECT COUNT(*) AS total_linhas,
# MAGIC        COUNT(DISTINCT moeda_codigo) AS moedas,
# MAGIC        COUNT(DISTINCT data_ref)     AS dias
# MAGIC FROM workspace.bronze_financeiro.taxas_historico_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity check: taxas médias por moeda
# MAGIC SELECT moeda_codigo, COUNT(*) AS dias,
# MAGIC        MIN(data_ref) AS de, MAX(data_ref) AS ate,
# MAGIC        ROUND(AVG(taxa_usd), 4) AS taxa_media_por_usd
# MAGIC FROM workspace.bronze_financeiro.taxas_historico_raw
# MAGIC GROUP BY moeda_codigo ORDER BY taxa_media_por_usd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirma o particionamento por ano_mes
# MAGIC SHOW PARTITIONS workspace.bronze_financeiro.taxas_historico_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Diagnóstico de rede (limitação do Free)

# COMMAND ----------

import socket
hosts = ["open.er-api.com", "raw.githubusercontent.com", "api.github.com",
         "api.frankfurter.app", "cdn.jsdelivr.net", "api.exchangerate.host"]
print("Alcançabilidade DNS de fontes de câmbio no Free Serverless:")
for h in hosts:
    try:
        print(f"  ✅ {h:28} → {socket.gethostbyname(h)}")
    except Exception as e:
        print(f"  ❌ {h:28} → {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Diagnóstico de da API

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🩺 Diagnóstico — fonte travada ou pipeline não executou?

# COMMAND ----------

import csv, io

# 1) Data mais recente disponível AGORA no CSV bruto do Fed H.10 (direto da fonte)
resp_diag = HTTP.get(CSV_HISTORICO, timeout=60)
resp_diag.raise_for_status()
reader_diag = csv.DictReader(io.StringIO(resp_diag.text))
datas_fonte = sorted({row["Date"] for row in reader_diag if row.get("Date")})
ultima_data_fonte = datas_fonte[-1] if datas_fonte else None

# 2) O que já está gravado na tabela (última data + última execução)
row_tabela = spark.sql(f"""
    SELECT MAX(data_ref) AS ultima_data_tabela,
           MAX(_ingested_at) AS ultima_execucao
    FROM {BRONZE}.taxas_historico_raw
""").collect()[0]

ultima_data_tabela = row_tabela["ultima_data_tabela"]
ultima_execucao    = row_tabela["ultima_execucao"]

print(f"📡 Última data disponível na FONTE (CSV agora)  : {ultima_data_fonte}")
print(f"🗄️  Última data já GRAVADA na tabela              : {ultima_data_tabela}")
print(f"🕒 Última vez que o pipeline ESCREVEU na tabela   : {ultima_execucao}")
print()

if ultima_data_fonte == ultima_data_tabela:
    print("✅ Diagnóstico: a fonte (Fed H.10 / GitHub) está travada nessa data.")
    print("   Não é um problema do seu pipeline — o CSV upstream ainda não tem dado mais novo.")
else:
    print("⚠️ Diagnóstico: a fonte JÁ tem uma data mais nova do que a gravada na tabela.")
    print("   O pipeline não está pegando o dado novo — vale investigar a execução (log, erros, cache).")
