# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

processed=process_path
source=raw_path

# COMMAND ----------

driver=dbutils.notebook.run("driver", 0)

# COMMAND ----------

# MAGIC %run "../ingestion/Constructors"

# COMMAND ----------

# MAGIC %run ../ingestion/ingest-circuit

# COMMAND ----------

# MAGIC %run ../ingestion/laptimes

# COMMAND ----------

# MAGIC %run ../ingestion/Pitstops

# COMMAND ----------

# MAGIC %run ../ingestion/qualifying

# COMMAND ----------

# MAGIC %run ../ingestion/races

# COMMAND ----------

# MAGIC %run ../ingestion/result
