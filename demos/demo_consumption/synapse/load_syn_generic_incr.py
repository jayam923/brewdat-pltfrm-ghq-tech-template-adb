# Databricks notebook source
import json

dbutils.widgets.text("brewdat_library_version", "v0.5.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_database", "gld_ghq_tech_demo_consumption", "2 - source_database")
source_database = dbutils.widgets.get("source_database")
print(f"{source_database = }")

dbutils.widgets.text("source_table", "monthly_sales_order", "3 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_table", "dbo.monthly_sales_order", "4 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "5 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text(
    "additional_parameters",
    """{
        "staging_table": "dbo.monthly_sales_order_stg",
        "ingestion_procedure": "dbo.sp_ingest_monthly_sales_order_stg"
    }""",
    "6 - additional_parameters",
)
additional_parameters = dbutils.widgets.get("additional_parameters")
additional_parameters = json.loads(additional_parameters)
print(f"{additional_parameters = }")

staging_table = additional_parameters.get("staging_table")
print(f"{staging_table = }")

ingestion_procedure = additional_parameters.get("ingestion_procedure")
print(f"{ingestion_procedure = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(common_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Service Principal to authenticate Databricks to both ADLS and a temporary Blob Storage location
common_utils.configure_spn_access_for_adls(
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
    assert staging_table
    assert ingestion_procedure

    effective_data_interval_end = (
        spark.read
        .table(f"`{source_database}`.`{source_table}`")
        .agg(F.max("__update_gmt_ts").cast("string"))
        .collect()[0][0]
    )
    print(f"{effective_data_interval_end = }")

    df = (
        spark.read
        .table(f"`{source_database}`.`{source_table}`")
        .filter(F.col("__update_gmt_ts").between(data_interval_start, effective_data_interval_end))
    )

    row_count = df.count()

    # Check that both staging and target tables exist and truncate staging table
    pre_actions = f"""
        IF OBJECT_ID('{staging_table}', 'U') IS NULL
            THROW 50000, 'Could not locate staging table: {staging_table}', 1;
        IF OBJECT_ID('{target_table}', 'U') IS NULL
            THROW 50000, 'Could not locate target table: {target_table}', 1;
        TRUNCATE TABLE {staging_table};
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
        .option("dbTable", staging_table)
        .option("tempDir", f"{synapse_blob_temp_root}/{staging_table}")
        .option("preActions", pre_actions)
        .option("postActions", post_actions)
        .save()
    )

except Exception:
    common_utils.exit_with_last_exception()

# COMMAND ----------

results = common_utils.ReturnObject(
    status=common_utils.RunStatus.SUCCEEDED,
    target_object=f"synapse/{staging_table}",
    num_records_read=row_count,
    num_records_loaded=row_count,
    effective_data_interval_start=data_interval_start,
    effective_data_interval_end=effective_data_interval_end or data_interval_start,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)