# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

# COMMAND ----------

data = [
    ("John", "New York", 30),
    ("Alice", "New York", 25),
    ("Bob", "Los Angeles", 35),
    ("Eve", "Los Angeles", 29),
    ("Mike", "Chicago", 40),
    ("Jane", "Chicago", 32),
]



columns = ["Name", "City", "Age"]
df = spark.createDataFrame(data, columns)




# COMMAND ----------



window_spec = Window.partitionBy("City").orderBy(col("Age").desc())


ranked_df = df.withColumn("Rank", rank().over(window_spec))


ranked_df.show()
