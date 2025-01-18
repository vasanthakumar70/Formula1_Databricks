# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_raw cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_processed cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_final cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use database f1_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(
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
# MAGIC     path 'abfss://raw@vasanthblob.dfs.core.windows.net/circuits.csv'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitId INT,
# MAGIC name STRING,
# MAGIC date DATE,
# MAGIC time STRING,
# MAGIC url STRING)
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@vasanthblob.dfs.core.windows.net/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC constructorId INT,
# MAGIC constructorRef STRING,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://raw@vasanthblob.dfs.core.windows.net/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC number INT,
# MAGIC code STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC dob DATE,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS (path "abfss://raw@vasanthblob.dfs.core.windows.net/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
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
# MAGIC OPTIONS(path "abfss://raw@vasanthblob.dfs.core.windows.net/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC driverId INT,
# MAGIC duration STRING,
# MAGIC lap INT,
# MAGIC milliseconds INT,
# MAGIC raceId INT,
# MAGIC stop INT,
# MAGIC time STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://raw@vasanthblob.dfs.core.windows.net/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@vasanthblob.dfs.core.windows.net/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
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
# MAGIC OPTIONS (path "abfss://raw@vasanthblob.dfs.core.windows.net/qualifying", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database f1_processed
# MAGIC managed location "abfss://processed@vasanthblob.dfs.core.windows.net"
