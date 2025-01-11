# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/mount_function

# COMMAND ----------

# MAGIC %md
# MAGIC races

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

constructor_schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

constructor=spark.read.format("json").schema(constructor_schema).load("/mnt/vasanthblob/raw/constructors.json")

# COMMAND ----------

constructor_renamed=constructor\
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .drop("url")
 


# COMMAND ----------

constructor_with_date=add_ingestion_date(constructor_renamed)

# COMMAND ----------

constructor_with_date.show(truncate=False)

# COMMAND ----------

constructor_with_date.write.format("parquet").mode("overwrite").save("/mnt/vasanthblob/processed/constructor")
