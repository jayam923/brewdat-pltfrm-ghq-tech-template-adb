# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_hive_database", "gld_ghq_tech_demo_consumption", "2 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"source_hive_database: {source_hive_database}")

dbutils.widgets.text("source_hive_table", "customer_orders", "3 - source_hive_table")
source_hive_table = dbutils.widgets.get("source_hive_table")
print(f"source_hive_table: {source_hive_table}")

# COMMAND ----------

import re
import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils

# Print a module's help
help(common_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Service Principal to authenticate Databricks to both ADLS and a temporary Blob Storage location
common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[adls_silver_gold_storage_account_name, synapse_blob_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# Service principal to authenticate Databricks to Azure Synapse Analytics (FROM EXTERNAL PROVIDER)
# For required database permissions, see:
# https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/synapse-analytics#required-azure-synapse-permissions-for-the-copy-statement
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", spn_client_id)
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", dbutils.secrets.get(scope=key_vault_name, key=spn_secret_name))

# COMMAND ----------

try:
    df = spark.read.table(f"{source_hive_database}.{source_hive_table}")

    row_count = df.count()
    target_table_name = f"dbo.stg_{source_hive_table}"

    # Both Service Principal and Synapse Managed Identity require
    # read/write access to the temporary Blob Storage location
    (
        df.write
        .format("com.databricks.spark.sqldw")
        .mode("overwrite")
        .option("url", synapse_connection_string)
        .option("enableServicePrincipalAuth", True)
        .option("useAzureMSI", True)
        .option("dbTable", target_table_name)
        .option("tableOptions", "HEAP")
        .option("tempDir", f"{synapse_blob_temp_root}/{source_hive_table}")
        .save()
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils)

# COMMAND ----------

try:
    target_server_name = re.search("sqlserver://(.*?):", synapse_connection_string).group(1)
    target_database_name = re.search(";database=(.*?);", synapse_connection_string).group(1)
    target_object = f"{target_server_name}.{target_database_name}.{target_table_name}"
except Exception:
    target_object = f"unknown_synapse.{target_table_name}"

results = common_utils.ReturnObject(
    status=common_utils.RunStatus.SUCCEEDED,
    target_object=target_object,
    num_records_read=row_count,
    num_records_loaded=row_count,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
