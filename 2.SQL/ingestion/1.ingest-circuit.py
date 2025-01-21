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
# MAGIC circuits

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                            StructField("circuitRef", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("location", StringType(), True),
                            StructField("country", StringType(), True),
                            StructField("lat", DoubleType(), True),
                            StructField("lng", DoubleType(), True),
                            StructField("alt", IntegerType(), True),
                            StructField("url", StringType(), True)
])

# COMMAND ----------

circuit=spark.read.format("csv").option("header","True").schema(circuits_schema).load(f"{raw_path}/{filename}/circuits.csv")

# COMMAND ----------

circuit_renamed=circuit\
    .withColumnRenamed("circuitid", "circuit_id")\
    .withColumnRenamed("circuitref", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")\
    .drop("url")

# COMMAND ----------

circuit_with_date=add_ingestion_date(circuit_renamed)

# COMMAND ----------

circuit_with_date.show(truncate=False)

# COMMAND ----------

if source_point=="adls":
    circuit_with_date.write.format("parquet").mode("overwrite").save(f"{process_path}/circuits")
elif source_point=="table":
    circuit_with_date.write.format("delta").mode("overwrite").saveAsTable("f1_processed.circuits")
