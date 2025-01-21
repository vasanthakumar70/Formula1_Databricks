# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/function_time

# COMMAND ----------

dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")


# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col,dense_rank,col
from pyspark.sql.window import Window

# COMMAND ----------

race_result=spark.read.format("delta").table(f"{presentation_database}.race_result").filter(col("filename")==filename)

# COMMAND ----------

window_spec=Window.partitionBy("race_year").orderBy(col("total_points").desc(),col("win").desc())
constructor_standing=race_result\
    .groupBy("team","race_year")\
    .agg(sum(col("points")).alias("total_points"),
             count(when(col("position") == 1,True)).alias("win"))\
    .withColumn("rank",dense_rank().over(window_spec))
    


# COMMAND ----------

merge_condition="s.team=t.team and s.race_year=t.race_year"

# COMMAND ----------

merge_table(constructor_standing,presentation_database,"constructor_standings","0",merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_final_inc.constructor_standings
