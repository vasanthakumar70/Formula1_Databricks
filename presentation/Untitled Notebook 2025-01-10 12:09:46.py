# Databricks notebook source
dbutils.fs.ls('/mnt/vasanthblob/processed/')

# COMMAND ----------

dbutils.widgets.text("process_path", "")
processed_path=dbutils.widgets.get("process_path")

# COMMAND ----------

race=spark.read.format("parquet").load(f"{processed_path}/race")
race_filtered=race.filter("race_year==2020").select("race_Id","circuit_id", "name", "race_timestamp", "race_year")

# COMMAND ----------

circuit=spark.read.format("parquet").load(f"{processed_path}/circuits")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_joined=race_filtered\
    .join(
        circuit, 
        race["circuit_id"]==circuit["circuit_id"], 
        how="inner")\
    .select("race_Id",race["circuit_id"], race["name"], "race_timestamp", "race_year",col("location").alias("circuit_location"))\
    .filter("location=='Abu Dhabi'")

# COMMAND ----------



# COMMAND ----------

result=spark.read.format("parquet").load(f"{processed_path}/results")
result_renamed=result\
    .select("race_id", "driver_id", "constructor_id","grid","position","laps","fastest_lap_time","points")

# COMMAND ----------

driver=spark.read.format("parquet").load(f"{processed_path}/driver")

# COMMAND ----------

driver_renamed=driver.select("driver_id", "name", "nationality","number")

# COMMAND ----------

constructor=spark.read.format("parquet").load(f"{processed_path}/constructor")

# COMMAND ----------

constructor_renamed=constructor.select("constructor_id", "name")

# COMMAND ----------

result_joined=result_renamed.alias("result")\
    .join(driver_renamed.alias("driver"), "driver_id")\
    .join(constructor_renamed.alias("constructor"), result_renamed["constructor_id"] == constructor_renamed["constructor_id"])\
    .select("result.*", "driver.*", "constructor.*")\
    .drop(constructor_renamed["constructor_id"],driver_renamed["driver_id"])

# COMMAND ----------

result_joined.show()

# COMMAND ----------

race_joined.join(
    result_joined,
    result_joined["race_id"]==race_joined["race_Id"]
)/
    .orderBy(col("points").desc()).show()
