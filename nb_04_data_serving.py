# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📤 Projeto 09 — Financeiro | nb_04_data_serving
# MAGIC Exporta a Gold `variacao_cambial_30d` para o **Supabase** (PostgreSQL) via JDBC, com senha no Infisical.
# MAGIC Fallback garantido em Volume/CSV dentro do Databricks (se o JDBC falhar por rede).

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

# Testa se o driver JDBC do Postgres já está disponível no classpath
try:
    spark._jvm.org.postgresql.Driver
    print("✅ Driver org.postgresql.Driver encontrado — não precisa instalar nada.")
except Exception as e:
    print("❌ Driver não encontrado:", e)

# COMMAND ----------

# MAGIC %pip install -q infisicalsdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

import os
from pyspark.sql import functions as F

log("serving", "Iniciando Data Serving")
df_gold = spark.table(f"{GOLD}.variacao_cambial_30d")

# ── 1) EXPORTAÇÃO JDBC (best-effort) → Supabase  (TODO 05) ────────────────
def exportar_jdbc(df) -> bool:
    try:
        from infisical_sdk import InfisicalSDKClient
        client = InfisicalSDKClient(host="https://app.infisical.com")
        client.auth.universal_auth.login(
            client_id=os.environ.get("INFISICAL_CLIENT_ID", ""),
            client_secret=os.environ.get("INFISICAL_CLIENT_SECRET", ""),
        )
        secret = client.secrets.get_secret(
            secret_name="POSTGRES_JDBC_URL",
            project_id=os.environ.get("INFISICAL_PROJECT_ID", ""),
            environment_slug="dev", secret_path="/",
        )
        jdbc_url = secret.secretValue   # ex.: jdbc:postgresql://...supabase.co:5432/postgres?...&sslmode=require

        (df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "variacao_cambial_30d")
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite").save())
        log("serving", "✅ Exportação JDBC (Supabase) concluída")
        return True
    except Exception as e:
        log("serving", f"⚠️ JDBC indisponível ({type(e).__name__}: {e}) — usando fallback")
        return False

exportar_jdbc(df_gold)

# ── 2) FALLBACK GARANTIDO — Volume Delta/CSV ──────────────────────────────
spark.sql(f"CREATE VOLUME IF NOT EXISTS {GOLD}.exports")
caminho = f"/Volumes/{CATALOG}/gold_{PROJECT}/exports/variacao_cambial_30d"
(df_gold.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminho))
log("serving", f"✅ CSV gravado em {caminho}")

# COMMAND ----------

display(df_gold.orderBy(F.desc("volatilidade")))
