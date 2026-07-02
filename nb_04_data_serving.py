# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📤 Projeto 09 — Financeiro | nb_04_data_serving
# MAGIC Exporta a Gold `variacao_cambial_30d` para o **Supabase** (PostgreSQL) via JDBC. Exporta a Gold via conector nativo postgresql (pooler Session mode), com senha no Databricks Secrets. Fallback em Volume/CSV..
# MAGIC Fallback garantido em Volume/CSV dentro do Databricks (se o JDBC falhar por rede).

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

# MAGIC %pip install -q infisicalsdk

# COMMAND ----------

from pyspark.sql import functions as F

log("serving", "Iniciando Data Serving")

def exportar_postgres(df, tabela) -> bool:
    try:
        (df.coalesce(1).write.format("postgresql")           # coalesce(1): 1 partição só
            .option("host", "aws-1-ca-central-1.pooler.supabase.com")
            .option("port", "5432")                          # ← SESSION mode (era 6543)
            .option("database", "postgres")
            .option("dbtable", tabela)
            .option("user", "postgres.gqurwswkaojrgqhcyfrq")
            .option("password", dbutils.secrets.get(scope="infisical", key="postgres_password"))
            .option("numpartitions", "1")                    # ← serializa a escrita
            .mode("overwrite").save())
        log("serving", f"✅ {tabela} → Supabase (session pooler)")
        return True
    except Exception as e:
        log("serving", f"⚠️ {tabela}: {type(e).__name__}: {e}")
        return False

TABELAS = [
    (GOLD,   "variacao_cambial_30d"),
    (SILVER, "fato_taxas_historico"),
    (GOLD,   "correlacao_moedas"),
    (GOLD,   "alertas_cambiais_brl"),
]

resultados = {}
for schema, tab in TABELAS:
    resultados[tab] = exportar_postgres(spark.table(f"{schema}.{tab}"), tab)

falhas = [t for t, ok in resultados.items() if not ok]
if falhas:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {GOLD}.exports")
    for schema, tab in TABELAS:
        if tab in falhas:
            caminho = f"/Volumes/{CATALOG}/gold_{PROJECT}/exports/{tab}"
            spark.table(f"{schema}.{tab}").coalesce(1).write.mode("overwrite").option("header","true").csv(caminho)
            log("serving", f"CSV fallback: {tab} → {caminho}")

log("serving", f"Concluído. Sucessos: {sum(resultados.values())}/{len(TABELAS)}")
