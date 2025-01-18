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
# MAGIC constructor

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

constructor=spark.read.format("json").schema(constructor_schema).load(f"{raw_path}/{filename}/constructors.json")

# COMMAND ----------

constructor_renamed=constructor\
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .drop("url")
 


# COMMAND ----------

constructor_with_date=add_ingestion_date(constructor_renamed,filename)

# COMMAND ----------

constructor_with_date.show(truncate=False)

# COMMAND ----------

if source_point=="adls":
    constructor_with_date.write.format("parquet").mode("overwrite").save(f"{process_path}/constructor")
elif source_point=="table":
    constructor_with_date.write.mode("overwrite").format("parquet")\
        .option("path", f"abfss://{container_name}@vasanthblob.dfs.core.windows.net/processed/constructor")\
            .saveAsTable(f"{process_database}.constructor")
    
