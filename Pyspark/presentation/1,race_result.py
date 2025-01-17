# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

processed_path=process_path
presentation_path=presentation_path

# COMMAND ----------

race=spark.read.format("parquet").load(f"{processed_path}/race")
race_filtered=race.filter("race_year==2020").select("race_Id","circuit_id", "name", "race_timestamp", "race_year")

# COMMAND ----------

circuit=spark.read.format("parquet").load(f"{processed_path}/circuits")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

race_joined=race_filtered\
    .join(
        circuit, 
        race["circuit_id"]==circuit["circuit_id"], 
        how="inner")\
    .select("race_Id",race["circuit_id"], race["name"].alias("Race_Name"), "race_timestamp", "race_year",col("location").alias("circuit_location"))\
   ## .filter("location=='Abu Dhabi'")

# COMMAND ----------

race_joined.show()

# COMMAND ----------

result=spark.read.format("parquet").load(f"{processed_path}/results")
result_renamed=result\
    .select("race_id", "driver_id", "constructor_id","grid","position","laps","fastest_lap_time","points")

# COMMAND ----------

driver=spark.read.format("parquet").load(f"{processed_path}/driver")

# COMMAND ----------

driver_renamed=driver.select("driver_id", col("name").alias("Driver"), "nationality","number")

# COMMAND ----------

constructor=spark.read.format("parquet").load(f"{processed_path}/constructor")

# COMMAND ----------

constructor_renamed=constructor.select("constructor_id", col("name").alias("Team"))

# COMMAND ----------

result_joined=result_renamed.alias("result")\
    .join(driver_renamed.alias("driver"), "driver_id")\
    .join(constructor_renamed.alias("constructor"), result_renamed["constructor_id"] == constructor_renamed["constructor_id"])\
    .select("result.*", "driver.*", "constructor.*")\
    .drop(constructor_renamed["constructor_id"],driver_renamed["driver_id"])

# COMMAND ----------

result_joined.show()

# COMMAND ----------

race_result=race_joined.join(
    result_joined,
    result_joined["race_id"]==race_joined["race_Id"]
)\
    .select("race_year","Race_name","race_timestamp","circuit_location",col("driver").alias("driver_name"),col("number").alias("driver_number"),col("nationality").alias("driver_nationality"),"team","grid","fastest_lap_time","points","position")\
    .withColumn("created_date",current_timestamp())


# COMMAND ----------

race_result.write.format("parquet").mode("overwrite").save(f"{presentation_path}/race_result")

# COMMAND ----------

race_result.display()
