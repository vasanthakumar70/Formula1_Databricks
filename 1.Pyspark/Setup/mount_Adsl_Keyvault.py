# Databricks notebook source
# MAGIC %md
# MAGIC ###key vault

# COMMAND ----------

client_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='clientid')
tentent_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='tententid')
screat_id=dbutils.secrets.get(scope="vasanth_keyvalt",key='secretidnew')

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": screat_id,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tentent_id}/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://container1@vasanthblob.dfs.core.windows.net/",
  mount_point = "/mnt/adls",
  extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("/mnt/adls")
