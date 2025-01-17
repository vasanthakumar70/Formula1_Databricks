# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.vasanthblob.dfs.core.windows.net",
    dbutils.secrets.get(scope="vasanth_keyvalt", key="blobaccess")
)

# COMMAND ----------

def delete_blob_files(container_name,folder):
    adls_folder_path = "abfss://{container_name}@vasanthblob.dfs.core.windows.net/{folder}"
    dbutils.fs.rm(adls_folder_path, True)


# COMMAND ----------

dbutils.widgets.text("container_name", "vasanth_container")
dbutils.widgets.text("folder", "vasanth_folder")
container_name = dbutils.widgets.get("container_name")
folder = dbutils.widgets.get("folder")

# COMMAND ----------

delete_blob_files(container_name,folder)
