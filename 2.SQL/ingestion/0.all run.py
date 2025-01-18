# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

processed=process_path
source=raw_path
source_path="table"

# COMMAND ----------

driver=dbutils.notebook.run("4.driver", 0,{"source_path":source_path})

# COMMAND ----------

# MAGIC %run "../ingestion/3.Constructors"

# COMMAND ----------

# MAGIC %run "../ingestion/1.ingest-circuit"

# COMMAND ----------

# MAGIC %run "../ingestion/7.laptimes"

# COMMAND ----------

# MAGIC %run "../ingestion/6.Pitstops"

# COMMAND ----------

# MAGIC %run "../ingestion/8.qualifying"

# COMMAND ----------

# MAGIC %run "../ingestion/2.races"

# COMMAND ----------

# MAGIC %run "../ingestion/5.result"
