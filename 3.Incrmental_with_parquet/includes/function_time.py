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

def updatepartitioncolumn(df,partition_column):
    columns=[]
    for column in df.columns:
        if column !=partition_column:
            columns.append(column)
    columns.append(partition_column)
    out_df=df.select(columns)

    return out_df 

# COMMAND ----------

def merge_table(df, db_name, table_name, process_path, partition_condition):
    from delta.tables import DeltaTable

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    if source_point == "adls":
        
        df.write.format("parquet").partitionBy(partition_condition).mode("overwrite").save(f"{process_path}/{table_name}")
    elif source_point == "table":
        
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
           
            df = updatepartitioncolumn(df, partition_condition)

            
            df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
            print("Table partitions updated.")
        else:
            
            df.write.partitionBy(partition_condition).format("parquet").mode("overwrite") \
                .option("path", f"abfss://{container_name}@vasanthblob.dfs.core.windows.net/{process_path}/{table_name}") \
                .saveAsTable(f"{db_name}.{table_name}")
            print("New table created.")

