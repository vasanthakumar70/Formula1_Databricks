# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_result=spark.read.format("parquet").load(f"{presentation_path}/race_result")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col,dense_rank
from pyspark.sql.window import Window

# COMMAND ----------

window_spec=Window.orderBy(col("total_points").desc(),col("win").desc())
race_result\
    .groupBy("team")\
    .agg(sum(col("points")).alias("total_points"),
             count(when(col("position") == 1,True)).alias("win"))\
    .withColumn("rank",dense_rank().over(window_spec))\
    .display()
    

