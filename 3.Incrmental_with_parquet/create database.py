# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists par_f1_raw_inc cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists par_f1_processed_inc cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists par_f1_final_inc cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists par_f1_raw_inc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use database par_f1_raw_inc

# COMMAND ----------

dbutils.widgets.text("filename", "2020-03-21")
filename=dbutils.widgets.get("filename")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.circuits(
# MAGIC     circuitId INT,
# MAGIC     circuitRef STRING,
# MAGIC     name STRING,
# MAGIC     location STRING,
# MAGIC     country STRING,
# MAGIC     lat DOUBLE,
# MAGIC     lng DOUBLE,
# MAGIC     alt INT,
# MAGIC     url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC     header "true",
# MAGIC     path 'abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/circuits.csv'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.races;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.races(raceId INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitId INT,
# MAGIC name STRING,
# MAGIC date DATE,
# MAGIC time STRING,
# MAGIC url STRING)
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.constructors(
# MAGIC constructorId INT,
# MAGIC constructorRef STRING,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.drivers(
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC number INT,
# MAGIC code STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC dob DATE,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS (path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.results;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.results(
# MAGIC resultId INT,
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC constructorId INT,
# MAGIC number INT,grid INT,
# MAGIC position INT,
# MAGIC positionText STRING,
# MAGIC positionOrder INT,
# MAGIC points INT,
# MAGIC laps INT,
# MAGIC time STRING,
# MAGIC milliseconds INT,
# MAGIC fastestLap INT,
# MAGIC rank INT,
# MAGIC fastestLapTime STRING,
# MAGIC fastestLapSpeed FLOAT,
# MAGIC statusId STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.pit_stops(
# MAGIC driverId INT,
# MAGIC duration STRING,
# MAGIC lap INT,
# MAGIC milliseconds INT,
# MAGIC raceId INT,
# MAGIC stop INT,
# MAGIC time STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.lap_times(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS par_f1_raw_inc.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS par_f1_raw_inc.qualifying(
# MAGIC constructorId INT,
# MAGIC driverId INT,
# MAGIC number INT,
# MAGIC position INT,
# MAGIC q1 STRING,
# MAGIC q2 STRING,
# MAGIC q3 STRING,
# MAGIC qualifyId INT,
# MAGIC raceId INT)
# MAGIC USING json
# MAGIC OPTIONS (path "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/raw/${filename}/qualifying", multiLine true)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists par_f1_processed_inc 
# MAGIC managed location "abfss://parquetincremental@vasanthblob.dfs.core.windows.net/processed/"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists par_f1_final_inc  cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists par_f1_final_inc 
# MAGIC
