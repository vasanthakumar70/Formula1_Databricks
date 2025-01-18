# Databricks notebook source
# MAGIC %run ../includes/function_time

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")


# COMMAND ----------


processed_path=process_path
presentation_path=presentation_path

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

race=spark.read.format("delta").table(f"{process_database}.race")
race_filtered=race.filter(col("filename")==filename).select("race_Id","circuit_id", "name", "race_timestamp", "race_year","filename")

# COMMAND ----------

circuit=spark.read.format("delta").table(f"{process_database}.circuits").filter(col("filename")==filename)

# COMMAND ----------

race_joined=race_filtered\
    .join(
        circuit, 
        race["circuit_id"]==circuit["circuit_id"], 
        how="inner")\
    .select("race_Id",race["circuit_id"], race["name"].alias("Race_Name"), "race_timestamp", "race_year",col("location").alias("circuit_location"),race["filename"])\
   ## .filter("location=='Abu Dhabi'")

# COMMAND ----------

race_joined.show()

# COMMAND ----------

result=spark.read.format("delta").table(f"{process_database}.results")
result_renamed=result\
    .filter(col("filename")==filename)\
    .select("race_id", "driver_id", "constructor_id","grid","position","laps","fastest_lap_time","points","filename")

# COMMAND ----------

driver=spark.read.format("delta").table(f"{process_database}.driver").filter(col("filename")==filename)

# COMMAND ----------

driver_renamed=driver.select("driver_id", col("name").alias("Driver"), "nationality","number").filter(col("filename")==filename)

# COMMAND ----------

constructor=spark.read.format("delta").table(f"{process_database}.constructor").filter(col("filename")==filename)

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
    .select(race_joined["race_Id"],"race_year","Race_name","race_timestamp","circuit_location",col("driver").alias("driver_name"),col("number").alias("driver_number"),col("nationality").alias("driver_nationality"),"team","grid","fastest_lap_time","points","position")\
    .withColumn("created_date",current_timestamp())\
    .withColumn("filename",lit(filename))


# COMMAND ----------

merge_condition="t.race_id=s.race_id and t.driver_name=s.driver_name"

# COMMAND ----------

merge_table(race_result,presentation_database,"race_result",presentation_path,merge_condition,"race_id")
