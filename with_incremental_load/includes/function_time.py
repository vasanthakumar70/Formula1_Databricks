# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

def add_ingestion_date(df,filename):
    output_df=df.withColumn("ingestion_date",current_timestamp())\
        .withColumn("filename",lit(filename))
    return output_df

# COMMAND ----------

source_point="table"

# COMMAND ----------


def merge_table(df,db_name,table_name,process_path,merge_condition,partition_condition):
    from delta.tables import DeltaTable
    if source_point=="adls":
        df.write.format("parquet").partitionBy("race_id").mode("overwrite").save(f"{process_path}/{table_name}")
    elif source_point=="table":
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            deltatable=DeltaTable.forName(spark,f"{db_name}.{table_name}")
            deltatable.alias("t")\
                .merge(df.alias("s"),merge_condition)\
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
            print("updated")

        else:
            df.write.partitionBy(partition_condition).format("delta").mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
            print("new table created")
