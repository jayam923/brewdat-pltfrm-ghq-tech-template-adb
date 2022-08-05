# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.3.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "adventureworks", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_adventureworks", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "sales_order_header", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

# COMMAND ----------

import os
import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, transform_utils, read_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

# Defining the service principal credentials for the Azure storage account
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "1d3aebfe-929c-4cc1-a988-31c040d2b798")
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope ="brewdatpltfrmghqtechakvd", key ="brewdat-spn-pltfrm-ghq-tech-template-rw-d"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/v2.0/token")

# Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "1d3aebfe-929c-4cc1-a988-31c040d2b798")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret",dbutils.secrets.get(scope ="brewdatpltfrmghqtechakvd", key ="brewdat-spn-pltfrm-ghq-tech-template-rw-d"))

# COMMAND ----------

df= spark.sql(f"SELECT * FROM slv_ghq_tech_adventureworks.{target_hive_table} WHERE __update_gmt_ts BETWEEN '{data_interval_start}' AND '{data_interval_end}'") 
        

# COMMAND ----------

{ 
df.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://brewdat-pltfrm-synwks-d.sql.azuresynapse.net:1433;database=poc_sqlpool;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated")
  .option("enableServicePrincipalAuth", "true")
  .option("useAzureMSI", "true")
  .option("dbTable", f"dbo.stg01_{target_hive_table}")
  .option("tableOptions", "HEAP")
  .option("tempDir", f"abfss://temp-csa@brewdatpltfrmsynwkssad.dfs.core.windows.net/{target_hive_table}")
  .mode("overwrite")
  .save()
}   
    
