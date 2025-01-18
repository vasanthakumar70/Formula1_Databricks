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
# MAGIC pitstops

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType,FloatType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

pit_stops_schema = StructType([StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pitshops=spark.read.format("json").schema(pit_stops_schema).option("multiline", "true").load(f"{raw_path}/{filename}/pit_stops.json")

# COMMAND ----------

pitshops.renamed = pitshops.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

pitshops_with_date=add_ingestion_date(pitshops.renamed,filename)

# COMMAND ----------

pitshops_with_date.show(truncate=False)

# COMMAND ----------

merge_condition="t.race_id=s.race_id and t.driver_id=s.driver_id and t.stop=s.stop"

# COMMAND ----------

merge_table(pitshops_with_date,process_database,"pitshops",process_path,merge_condition,"race_id")
