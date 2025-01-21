# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

dbutils.widgets.text("source_point","table")
source_point=dbutils.widgets.get("source_point")
dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")

# COMMAND ----------

# MAGIC %md
# MAGIC result

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType,FloatType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results=spark.read.format("json").schema(results_schema).load(f"{raw_path}/{filename}/results.json")

# COMMAND ----------

results_renamed = results.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

results_with_date=add_ingestion_date(results_renamed,filename)

# COMMAND ----------

results_with_date.show(truncate=False)

# COMMAND ----------

results_with_date.dropDuplicates(["race_id","driver_id"])

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

spark.conf.set("spark.databricks.optimize.dynamicPartitionPruning", "true")

# COMMAND ----------

merge_table(results_with_date,"f1_processed_inc","results",process_path,"t.result_id=s.result_id and t.race_id=s.race_id","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed_inc.results
# MAGIC group by race_id
# MAGIC order by race_id desc
# MAGIC
