# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

driver_stand=spark.read.format("parquet").load(f"{presentation_path}/race_result")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col

# COMMAND ----------

driver_agg=driver_stand\
    .groupBy("race_year","driver_name","driver_nationality","team")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position")==1,True)).alias("wins"))\



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,dense_rank

# COMMAND ----------

window_spec=Window.orderBy(col("total_points").desc(),col("wins").desc())

# COMMAND ----------

driver_rank=driver_agg\
    .withColumn("rank",dense_rank().over(window_spec))


# COMMAND ----------

driver_rank.write.format("parquet").mode("overwrite").save(f"{presentation_path}/driver_standings")
