# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC qualifying

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType,FloatType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying=spark.read.format("json").option("multiLine","True").schema(qualifying_schema).load(f"{raw_path}/qualifying")

# COMMAND ----------

qualifying_renamed = qualifying.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_with_date = add_ingestion_date(qualifying_renamed)

# COMMAND ----------

qualifying_with_date.show()

# COMMAND ----------

qualifying_with_date.write.format("parquet").mode("overwrite").save(f"{process_path}/qualifying")
