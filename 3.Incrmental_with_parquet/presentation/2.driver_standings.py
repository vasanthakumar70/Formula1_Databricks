# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/function_time

# COMMAND ----------

dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")


# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col

# COMMAND ----------

driver_stand=spark.read.format("delta").table(f"{presentation_database}.race_result").filter(col("filename")==filename)

# COMMAND ----------

driver_agg=driver_stand\
    .groupBy("race_year","driver_name","driver_nationality")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position")==1,True)).alias("wins"))\



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,dense_rank

# COMMAND ----------

window_spec=Window.partitionBy("race_year").orderBy(col("total_points").desc(),col("wins").desc())

# COMMAND ----------

driver_rank=driver_agg\
    .withColumn("rank",dense_rank().over(window_spec))


# COMMAND ----------

merge_condition="s.driver_name=t.driver_name and s.race_year=t.race_year"

# COMMAND ----------

merge_table(driver_rank,presentation_database,"driver_standings","0",merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_final_inc.driver_standings
