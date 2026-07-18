# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📈 Projeto 09 — Financeiro | nb_02_silver
# MAGIC **SCD Tipo 2** em `dim_moeda_cambio`: nova versão só quando a variação > `THRESHOLD_CAMBIAL`%.
# MAGIC **Fato histórico** em `fato_taxas_historico`, atualizado via `MERGE` (idempotente).

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

log("silver", "Iniciando Silver")

TGT_DIM  = f"{SILVER}.dim_moeda_cambio"
TGT_FATO = f"{SILVER}.fato_taxas_historico"

def preparar_src(raw_atual_df):
    """Snapshot mais recente por moeda, vs. USD (a dimensão é sempre expressa em USD)."""
    return (
        raw_atual_df
        .filter(F.col("base_moeda") == "USD") # dimensao sempre em USD
        .select("moeda_codigo", "taxa_usd", "base_moeda",
                F.try_to_timestamp("data_ref").cast("date").alias("data_ref"))
        .filter(F.col("data_ref").isNotNull())
        .dropDuplicates(["moeda_codigo"]) # uma linha por moeda
    )

def aplicar_scd2(src_df, tgt_table: str, threshold_pct: float = THRESHOLD_CAMBIAL):
    """SCD Tipo 2 com threshold — função pura, reutilizável em produção e em testes."""
    src = (
        src_df
        .withColumn("eff_start", F.current_date()) # inicio da validade
        .withColumn("eff_end", F.lit("9999-12-31").cast("date")) # fim 'infinito'
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(tgt_table):
        src.write.format("delta").partitionBy("is_current").saveAsTable(tgt_table)  
        log("silver", "dim_moeda_cambio criada (carga inicial)", src.count())
        return

    existing = spark.table(tgt_table).filter("is_current = true").select("moeda_codigo", "taxa_usd")

    with_old = (
        src.alias("s").join(existing.alias("old"), on="moeda_codigo", how="inner")
        .withColumn("variacao_pct",
            F.abs((F.col("s.taxa_usd") - F.col("old.taxa_usd")) / F.col("old.taxa_usd") * 100))
        .filter(F.col("variacao_pct") > threshold_pct) # só mudanças maiores que 2%
    )

    # Materializa ANTES do MERGE para evitar releitura da própria tabela alvo
    novas = (
        with_old.select("moeda_codigo", F.col("s.taxa_usd").alias("taxa_usd"), "s.base_moeda",
                         "s.data_ref", "s.eff_start", "s.eff_end", "s.is_current")
        .localCheckpoint(eager=True)
    )

    qtd = novas.count()
    if qtd == 0:
        log("silver", f"Nenhuma moeda variou > {threshold_pct}% — dim_moeda_cambio inalterada")
        return

    (DeltaTable.forName(spark, tgt_table).alias("t")
        .merge(novas.alias("s"), "t.moeda_codigo = s.moeda_codigo AND t.is_current = true")
        .whenMatchedUpdate(set={"t.eff_end": "current_date()", "t.is_current": "false"})
        .execute())
    novas.write.format("delta").mode("append").partitionBy("is_current").saveAsTable(tgt_table)
    log("silver", f"dim_moeda_cambio SCD2 atualizada (threshold {threshold_pct}%)", qtd)

# ── PARTE 1: dim_moeda_cambio ─────────────────────────────────────────────
raw_atual = spark.table(f"{BRONZE}.taxas_atual_raw")
src_dim = preparar_src(raw_atual)
aplicar_scd2(src_dim, TGT_DIM)

def coluna_regime(data_col):
    """Classifica cada data em pré/durante/pós-pandemia a partir de REGIMES (nb_00)."""
    it = iter(REGIMES)
    nome, ini, fim = next(it)
    expr = F.when((data_col >= F.lit(ini).cast("date")) &
                  (data_col <= F.lit(fim).cast("date")), F.lit(nome))
    for nome, ini, fim in it:
        expr = expr.when((data_col >= F.lit(ini).cast("date")) &
                         (data_col <= F.lit(fim).cast("date")), F.lit(nome))
    return expr.otherwise(F.lit("indefinido"))

# ── PARTE 2: fato_taxas_historico (MERGE incremental, idempotente) ────────
raw_hist = spark.table(f"{BRONZE}.taxas_historico_raw")
fato_novo = (
    raw_hist
    .select("moeda_codigo", "base_moeda",
            F.to_date("data_ref", "yyyy-MM-dd").alias("data"),
            F.col("taxa_usd").cast("double"))
    .dropDuplicates(["moeda_codigo", "data"])
    .withColumn("regime", coluna_regime(F.col("data")))  
    .withColumn("_processado_em", F.current_timestamp())
)

if not spark.catalog.tableExists(TGT_FATO):
    fato_novo.write.format("delta").partitionBy("moeda_codigo").saveAsTable(TGT_FATO)   # TODO 03.3
    log("silver", "fato_taxas_historico criado", fato_novo.count())
else:
    (DeltaTable.forName(spark, TGT_FATO).alias("t")
        .merge(fato_novo.alias("s"), "t.moeda_codigo = s.moeda_codigo AND t.data = s.data")
        .whenNotMatchedInsertAll().execute())
    log("silver", "fato_taxas_historico atualizado via MERGE", fato_novo.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verificações do Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estado atual da dimensão (1 versão por moeda na 1ª execução)
# MAGIC SELECT moeda_codigo, ROUND(taxa_usd,4) AS taxa_usd, is_current, eff_start, eff_end
# MAGIC FROM workspace.silver_financeiro.dim_moeda_cambio
# MAGIC ORDER BY moeda_codigo, eff_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS workspace.silver_financeiro.dim_moeda_cambio;   -- is_current=true

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS workspace.silver_financeiro.fato_taxas_historico;  -- 1 partição por moeda

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Testes do SCD2 — isolados por widget
# MAGIC O `nb_05_master` mantém `RUN_SCD2_TESTS=false`, então a orquestração **nunca** corrompe a dimensão real.

# COMMAND ----------

dbutils.widgets.dropdown("RUN_SCD2_TESTS", "false", ["true", "false"], "Executar testes SCD2?")
RUN_TESTS = dbutils.widgets.get("RUN_SCD2_TESTS") == "true"

# COMMAND ----------

if RUN_TESTS:
    # — variação > 2% deve gerar 2ª versão
    log("silver-test", "Simulando BRL +5% (> threshold) — deve criar 2ª versão")
    src_sim = src_dim.withColumn("taxa_usd",
        F.when(F.col("moeda_codigo") == "BRL", F.col("taxa_usd") * 1.05).otherwise(F.col("taxa_usd")))
    aplicar_scd2(src_sim, TGT_DIM)
    spark.table(TGT_DIM).filter("moeda_codigo='BRL'") \
        .select("taxa_usd", "is_current", "eff_start", "eff_end").orderBy("eff_start").show()

    # — variação < 2% NÃO deve gerar nova versão
    log("silver-test", "Simulando EUR +0.5% (< threshold) — NÃO deve criar versão")
    antes = spark.table(TGT_DIM).filter("moeda_codigo='EUR'").count()
    src_sim_eur = src_dim.withColumn("taxa_usd",
        F.when(F.col("moeda_codigo") == "EUR", F.col("taxa_usd") * 1.005).otherwise(F.col("taxa_usd")))
    aplicar_scd2(src_sim_eur, TGT_DIM)
    depois = spark.table(TGT_DIM).filter("moeda_codigo='EUR'").count()
    print("✅ PASSOU" if antes == depois else "❌ FALHOU", f"| EUR antes={antes} depois={depois}")
else:
    log("silver-test", "Testes SCD2 desativados (RUN_SCD2_TESTS=false)")
