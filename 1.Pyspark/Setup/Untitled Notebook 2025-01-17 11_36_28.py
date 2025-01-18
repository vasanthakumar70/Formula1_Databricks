# Databricks notebook source
# MAGIC %sql
# MAGIC drop database f1_processed cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database f1_processed
# MAGIC managed location "/mnt/vasanthblob/processed"
