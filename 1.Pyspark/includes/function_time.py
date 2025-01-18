# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(df):
    output_df=df.withColumn("ingestion_date",current_timestamp())
    return output_df
