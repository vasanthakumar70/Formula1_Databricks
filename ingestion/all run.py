# Databricks notebook source
dbutils.widgets.text("processed_path", "")
dbutils.widgets.text("source_path", "")
processed=dbutils.widgets.get("processed_path")
source=dbutils.widgets.get("source_path")

# COMMAND ----------

driver=dbutils.notebook.run("driver", 0,{"processed_path":processed,"source_path":source})

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
