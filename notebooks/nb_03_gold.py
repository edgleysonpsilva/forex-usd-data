# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📈 Projeto 09 — Financeiro | nb_03_gold
# MAGIC Tabelas: `variacao_cambial_30d`, `ranking_desvalorizacao`, `alertas_cambiais_brl`, `correlacao_moedas`

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

log("gold", "Iniciando Gold")

fato = spark.table(f"{SILVER}.fato_taxas_historico")
assert_not_empty(fato, "fato_taxas_historico")

# Window function LAG para variação diária 
w_moeda = Window.partitionBy("moeda_codigo").orderBy("data")
fato_var = (
    fato
    .withColumn("taxa_anterior", F.lag("taxa_usd").over(w_moeda))
    .withColumn("variacao_diaria_pct",
        F.round((F.col("taxa_usd") - F.col("taxa_anterior")) / F.col("taxa_anterior") * 100, 4))
    .filter(F.col("taxa_anterior").isNotNull())
)

# ── Gold 1 — Variação agregada 30d  ────────────────────────────
# taxa_atual = valor do último dia: usa last() na janela ordenada
w_ult = Window.partitionBy("moeda_codigo").orderBy(F.desc("data"))
variacao = (
    fato_var
    .withColumn("rn", F.row_number().over(w_ult))
    .groupBy("moeda_codigo")
    .agg(
        F.max(F.when(F.col("rn") == 1, F.col("taxa_usd"))).alias("taxa_atual"),
        F.round(F.avg("variacao_diaria_pct"), 4).alias("variacao_media_diaria"),
        F.round(F.max("variacao_diaria_pct"), 4).alias("variacao_max_diaria"),
        F.round(F.min("variacao_diaria_pct"), 4).alias("variacao_min_diaria"),
        F.round(F.stddev("variacao_diaria_pct"), 4).alias("volatilidade"),
        F.count("data").alias("dias_analisados"),
    )
    .withColumn("_updated_at", F.current_timestamp())
)
(variacao.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("moeda_codigo").saveAsTable(f"{GOLD}.variacao_cambial_30d"))      
log("gold", "variacao_cambial_30d salvo", variacao.count())

# ── Gold 2 — Ranking de desvalorização vs USD  ─────────────────
ranking = (
    variacao
    .withColumn("rank_desvalorizacao", F.rank().over(Window.orderBy(F.col("variacao_media_diaria").asc())))
    .select("moeda_codigo", "taxa_atual", "variacao_media_diaria", "volatilidade", "rank_desvalorizacao")
    .withColumn("_updated_at", F.current_timestamp())
)
(ranking.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.ranking_desvalorizacao"))
log("gold", "ranking_desvalorizacao salvo", ranking.count())

# ── Gold 3 — Alertas cambiais BRL  ─────────────────────────────
ALERTA_PCT = -3.0
alertas = (
    fato_var
    .filter((F.col("moeda_codigo") == "BRL") & (F.col("variacao_diaria_pct") < ALERTA_PCT))
    .select("data", "moeda_codigo", "taxa_usd", "variacao_diaria_pct")
    .withColumn("flag", F.lit("ALERTA_CAMBIAL"))
)
(alertas.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.alertas_cambiais_brl"))
log("gold", f"alertas_cambiais_brl salvo ({alertas.count()} alertas)")

# ── Gold 4 — Correlação entre moedas  ──
# Substitui a API de commodities (bloqueada por DNS no Free) por correlação intra-dataset.
base_brl = (fato_var.filter("moeda_codigo='BRL'")
            .select(F.col("data"), F.col("variacao_diaria_pct").alias("var_brl")))
outras = (fato_var.filter("moeda_codigo <> 'BRL'")
          .select("moeda_codigo", "data", F.col("variacao_diaria_pct").alias("var_moeda")))
correl = (
    outras.join(base_brl, on="data", how="inner")
    .groupBy("moeda_codigo")
    .agg(F.round(F.corr("var_moeda", "var_brl"), 4).alias("correlacao_com_brl"),
         F.count("data").alias("dias"))
    .withColumn("_updated_at", F.current_timestamp())
)
(correl.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.correlacao_moedas"))
log("gold", "correlacao_moedas salvo", correl.count())

display(variacao.orderBy(F.desc("volatilidade")))

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH variacao_diaria AS (
# MAGIC   SELECT
# MAGIC     moeda_codigo,
# MAGIC     data,
# MAGIC     taxa_usd,
# MAGIC     LAG(taxa_usd) OVER (PARTITION BY moeda_codigo ORDER BY data) AS taxa_anterior,
# MAGIC     ROUND(
# MAGIC       (taxa_usd - LAG(taxa_usd) OVER (PARTITION BY moeda_codigo ORDER BY data))
# MAGIC       / LAG(taxa_usd) OVER (PARTITION BY moeda_codigo ORDER BY data) * 100,
# MAGIC     4) AS variacao_diaria_pct
# MAGIC   FROM workspace.silver_financeiro.fato_taxas_historico
# MAGIC ),
# MAGIC
# MAGIC variacao_valida AS (
# MAGIC   SELECT * FROM variacao_diaria WHERE taxa_anterior IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC rank_max AS (
# MAGIC   SELECT
# MAGIC     moeda_codigo, data AS data_max, variacao_diaria_pct AS variacao_max_diaria,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY moeda_codigo ORDER BY variacao_diaria_pct DESC) AS rn_max
# MAGIC   FROM variacao_valida
# MAGIC ),
# MAGIC
# MAGIC rank_min AS (
# MAGIC   SELECT
# MAGIC     moeda_codigo, data AS data_min, variacao_diaria_pct AS variacao_min_diaria,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY moeda_codigo ORDER BY variacao_diaria_pct ASC) AS rn_min
# MAGIC   FROM variacao_valida
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   mx.moeda_codigo,
# MAGIC   mx.data_max      AS dia_variacao_maxima,
# MAGIC   mx.variacao_max_diaria,
# MAGIC   mn.data_min       AS dia_variacao_minima,
# MAGIC   mn.variacao_min_diaria
# MAGIC FROM rank_max mx
# MAGIC JOIN rank_min mn USING (moeda_codigo)
# MAGIC WHERE mx.rn_max = 1 AND mn.rn_min = 1
# MAGIC ORDER BY mx.moeda_codigo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verificações do Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- moedas por volatilidade (mais instável primeiro)
# MAGIC SELECT moeda_codigo, ROUND(taxa_atual,4) AS taxa_atual,
# MAGIC        variacao_media_diaria, variacao_max_diaria, variacao_min_diaria,
# MAGIC        volatilidade, dias_analisados
# MAGIC FROM workspace.gold_financeiro.variacao_cambial_30d
# MAGIC ORDER BY volatilidade DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ranking de desvalorização vs USD (rank 1 = mais desvalorizou)
# MAGIC SELECT rank_desvalorizacao AS rank, moeda_codigo,
# MAGIC        ROUND(taxa_atual,4) AS taxa_atual, variacao_media_diaria, volatilidade
# MAGIC FROM workspace.gold_financeiro.ranking_desvalorizacao
# MAGIC ORDER BY rank_desvalorizacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alertas de queda forte do BRL (> 3% em 1 dia)
# MAGIC SELECT * FROM workspace.gold_financeiro.alertas_cambiais_brl ORDER BY data DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- correlação de cada moeda com o BRL
# MAGIC SELECT moeda_codigo, correlacao_com_brl, dias
# MAGIC FROM workspace.gold_financeiro.correlacao_moedas
# MAGIC ORDER BY correlacao_com_brl DESC;
