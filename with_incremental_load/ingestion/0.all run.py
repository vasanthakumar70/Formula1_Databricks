# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_processed_inc cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_processed_inc 
# MAGIC managed location "/mnt/vasanthblob/incrementalload/processed/"

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")

# COMMAND ----------

processed=process_path
source=raw_path
source_point="table"


# COMMAND ----------

circuit=dbutils.notebook.run("1.ingest-circuit", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

race=dbutils.notebook.run("2.races", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

constructor=dbutils.notebook.run("3.Constructors", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

driver=dbutils.notebook.run("4.driver", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

result=dbutils.notebook.run("5.result", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

pitstops=dbutils.notebook.run("6.Pitstops", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

qualifying=dbutils.notebook.run("7.laptimes", 0,{"source_point":source_point,"filename":filename})

# COMMAND ----------

driver=dbutils.notebook.run("8.qualifying", 0,{"source_point":source_point,"filename":filename})
