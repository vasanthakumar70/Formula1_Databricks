# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    
    client_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='clientid')
    tentent_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='tententid')
    screat_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='secretidnew')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": screat_id,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tentent_id}/oauth2/token"}
    
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    

    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    

# COMMAND ----------

mount_adls("vasanthblob", "incrementalload")
