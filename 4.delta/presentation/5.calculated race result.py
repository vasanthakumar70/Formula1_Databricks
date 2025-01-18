# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")


# COMMAND ----------

from pyspark.sql.functions import lit,col

# COMMAND ----------

result = spark.read.table(f"{process_database}.results").select("result_id","position","points","constructor_id","race_id","driver_id").filter(col("position")<=10).filter(col("filename")==filename)
drivers = spark.read.table(f"{process_database}.driver").select("driver_id","name").filter(col("filename")==filename)
constructors = spark.read.table(f"{process_database}.constructor").select("constructor_id","name").filter(col("filename")==filename)
races = spark.read.table(f"{process_database}.race").select("race_id","name","race_year").filter(col("filename")==filename)

# COMMAND ----------



final_result = result\
    .join(drivers, result.driver_id == drivers.driver_id, "inner")\
    .join(constructors, result.constructor_id == constructors.constructor_id, "inner")\
    .join(races, result.race_id == races.race_id, "inner")\
    .select(races.race_year, constructors.name.alias("constructor_name"), drivers.name.alias("driver_name"), result.position, result.points, (lit(11) - result.position).alias("calculated_points"))\
    .withColumn("filename",lit(filename))



# COMMAND ----------

merge_condition="s.race_year=r.race_year and s.driver_name=r.driver_name and s.constructor_name=r.constructor_name and s.filename=r.filename"

# COMMAND ----------

merge_table(final_result,presentation_database,"calculated_result","0",merge_condition,"race_year")

# COMMAND ----------

final_result.display()
