# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/mount_function

# COMMAND ----------

dbutils.widgets.text("source_path","")
dbutils.widgets.text("processed_path","")
source_path=dbutils.widgets.get("source_path")
processed_path=dbutils.widgets.get("processed_path")

# COMMAND ----------

# MAGIC %md
# MAGIC races

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]), True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers=spark.read.format("json").schema(drivers_schema).load(f"{source_path}/drivers.json")

# COMMAND ----------

driver_renamed=drivers.withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("driverRef", "driver_ref") \
                            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                            .drop("url")

# COMMAND ----------

driver_with_date=add_ingestion_date(driver_renamed)

# COMMAND ----------

driver_with_date.show(truncate=False)

# COMMAND ----------

driver_with_date.write.format("parquet").mode("overwrite").save(f"{processed_path}/driver")

# COMMAND ----------

dbutils.notebook.exit("Success")
