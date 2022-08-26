# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_object", "gld_ghq_tech_demo_consumption.monthly_sales_order", "2 - source_object")
source_object = dbutils.widgets.get("source_object")
print(f"source_object: {source_object}")

dbutils.widgets.text("staging_object", "dbo.monthly_sales_order_stg", "3 - staging_object")
staging_object = dbutils.widgets.get("staging_object")
print(f"staging_object: {staging_object}")

dbutils.widgets.text("target_object", "dbo.monthly_sales_order", "4 - target_object")
target_object = dbutils.widgets.get("target_object")
print(f"target_object: {target_object}")

dbutils.widgets.text("ingestion_procedure", "dbo.sp_ingest_monthly_sales_order_stg", "5 - ingestion_procedure")
ingestion_procedure = dbutils.widgets.get("ingestion_procedure")
print(f"ingestion_procedure: {ingestion_procedure}")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "6 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

# COMMAND ----------

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

from pyspark.sql import functions as F

try:
    effective_data_interval_end = (
        spark.read
        .table(source_object)
        .agg(F.max("__update_gmt_ts").cast("string"))
        .collect()[0][0]
    )
    print(f"effective_data_interval_end: {effective_data_interval_end}")

    df = (
        spark.read
        .table(source_object)
        .filter(F.col("__update_gmt_ts").between(data_interval_start, effective_data_interval_end))
    )

    row_count = df.count()

    # Check that both staging and target tables exist and truncate staging table
    pre_actions = f"""
        IF OBJECT_ID('{staging_object}', 'U') IS NULL
            THROW 50000, 'Could not locate staging table: {staging_object}', 1;
        IF OBJECT_ID('{target_object}', 'U') IS NULL
            THROW 50000, 'Could not locate target table: {target_object}', 1;
        TRUNCATE TABLE {staging_object};
    """

    # Invoke procedure to upsert target table with staging data
    # Should use update/insert commands instead of merge
    post_actions = f"EXEC {ingestion_procedure};"

    # Both Service Principal and Synapse Managed Identity require
    # read/write access to the temporary Blob Storage location
    # Also, remember to create a Lifecycle Management policy to
    # delete temporary files older than 5 days
    (
        df.write
        .format("com.databricks.spark.sqldw")
        .mode("append")
        .option("url", synapse_connection_string)
        .option("enableServicePrincipalAuth", True)
        .option("useAzureMSI", True)
        .option("dbTable", staging_object)
        .option("tempDir", f"{synapse_blob_temp_root}/{staging_object}")
        .option("preActions", pre_actions)
        .option("postActions", post_actions)
        .save()
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

results = common_utils.ReturnObject(
    status=common_utils.RunStatus.SUCCEEDED,
    target_object=f"synapse/{target_object}",
    num_records_read=row_count,
    num_records_loaded=row_count,
    effective_data_interval_start=data_interval_start,
    effective_data_interval_end=effective_data_interval_end or data_interval_start,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
