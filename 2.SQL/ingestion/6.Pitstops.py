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

dbutils.fs.ls("/mnt/vasanthblob/raw/")

# COMMAND ----------

pitshops=spark.read.format("json").schema(pit_stops_schema).option("multiline", "true").load(f"{raw_path}/{filename}/pit_stops.json")

# COMMAND ----------

pitshops.renamed = pitshops.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

pitshops_with_date=add_ingestion_date(pitshops.renamed)

# COMMAND ----------

pitshops_with_date.show(truncate=False)

# COMMAND ----------

if source_point=="adls":
    pitshops_with_date.write.format("parquet").mode("overwrite").save(f"{process_path}/pitshops")
elif source_point=="table":
    pitshops_with_date.write.mode("overwrite").saveAsTable("f1_processed.pitshops")
