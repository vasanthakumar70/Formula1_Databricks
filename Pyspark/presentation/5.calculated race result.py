# Databricks notebook source
from pyspark.sql.functions import lit,col

# COMMAND ----------

result = spark.read.table("f1_processed.results").select("result_id","position","points","constructor_id","race_id","driver_id").filter(col("position")<=10)
drivers = spark.read.table("f1_processed.driver").select("driver_id","name")
constructors = spark.read.table("f1_processed.constructor").select("constructor_id","name")
races = spark.read.table("f1_processed.race").select("race_id","name","race_year")

# COMMAND ----------



final_result = result\
    .join(drivers, result.driver_id == drivers.driver_id, "inner")\
    .join(constructors, result.constructor_id == constructors.constructor_id, "inner")\
    .join(races, result.race_id == races.race_id, "inner")\
    .select(races.race_year, constructors.name.alias("constructor_name"), drivers.name.alias("driver_name"), result.position, result.points, (lit(11) - result.position).alias("calculated_points"))

final_result.write.saveAsTable("f1_final.calculated_race_result")

# COMMAND ----------

final_result.display()
