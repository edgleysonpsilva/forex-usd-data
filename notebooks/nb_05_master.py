# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Projeto 09 — Financeiro | nb_05_master
# MAGIC Orquestração sequencial via `%run` (Databricks Free não tem Jobs/Workflows).
# MAGIC Os testes de simulação do SCD2 (`nb_02_silver`) ficam desligados por padrão (widget `RUN_SCD2_TESTS=false`),
# MAGIC então este master **não corrompe** a dimensão com dados simulados.

# COMMAND ----------

# MAGIC %run ./nb_00_config

# COMMAND ----------

log("master", "Iniciando pipeline completo...")

# COMMAND ----------

# MAGIC %run ./nb_01_bronze

# COMMAND ----------

# MAGIC %run ./nb_02_silver

# COMMAND ----------

# MAGIC %run ./nb_03_gold

# COMMAND ----------

# MAGIC %run ./nb_04_data_serving

# COMMAND ----------

log("master", "Pipeline executado com sucesso de ponta a ponta!")
