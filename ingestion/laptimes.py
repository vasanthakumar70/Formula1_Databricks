# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/mount_function

# COMMAND ----------

# MAGIC %md
# MAGIC result

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType,FloatType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times=spark.read.format("csv").schema(lap_times_schema).load("/mnt/vasanthblob/raw/lap_times")

# COMMAND ----------

lap_times = lap_times.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_times_with_date=add_ingestion_date(lap_times)

# COMMAND ----------

lap_times_with_date.show(truncate=False)

# COMMAND ----------

lap_times_with_date.write.format("parquet").mode("overwrite").save("/mnt/vasanthblob/processed/lap_times")