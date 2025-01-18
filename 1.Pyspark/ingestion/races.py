# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC races

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType
from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)]) 

# COMMAND ----------

races=spark.read.format("csv").option("header","True").schema(races_schema).load(f"{raw_path}/races.csv")

# COMMAND ----------

races_renamed=races\
    .withColumnRenamed("raceId", "race_Id")\
    .withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("year","race_year")\
    .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))\
    .drop("date","time","url")


# COMMAND ----------

race_with_date=add_ingestion_date(races_renamed)

# COMMAND ----------

race_with_date.show(truncate=False)

# COMMAND ----------

race_with_date.write.format("parquet").partitionBy("race_year").mode("overwrite").save(f"{process_path}/race")
