# Databricks notebook source
calculated_race_result=spark.read.table("f1_final.calculated_race_result")

# COMMAND ----------

from pyspark.sql.functions import sum,count,avg,round,col

# COMMAND ----------

calculated_race_result\
    .groupBy("constructor_name")\
    .agg(count("*").alias("total_races"), 
         round(sum("points"),2).alias("total_points"), 
         round(avg("points"),2).alias("avg_points"))\
    .filter(col("total_races")>100)\
    .orderBy(col("avg_points").desc())\
    .display()

# COMMAND ----------

calculated_race_result\
    .filter("race_year between 2010 and 2020")\
    .groupBy("constructor_name")\
    .agg(count("*").alias("total_races"), 
         sum("points").alias("total_points"), 
         round(avg("points"),2).alias("avg_points"))\
    .filter(col("total_races")>100)\
    .orderBy(col("avg_points").desc())\
    .display()
