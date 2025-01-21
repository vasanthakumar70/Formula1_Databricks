# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_processed_inc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_final_inc

# COMMAND ----------

# DBTITLE 1,e
# MAGIC %sql
# MAGIC create database if not exists f1_raw_inc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_processed_inc

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_final_inc
