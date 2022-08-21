# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "sap_europe", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("source_table", "KNA1", "03 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"source_table: {source_table}")

dbutils.widgets.text("target_zone", "ghq", "04 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "05 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_sap_europe", "06 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "kna1", "07 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(common_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[
        adls_raw_bronze_storage_account_name,
        adls_brewdat_ghq_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

sap_sid = source_system_to_sap_sid.get(source_system)
attunity_sap_prelz_root = f"/attunity_sap/attunity_sap_{sap_sid}_prelz/prelz_sap_{sap_sid}"
print(f"attunity_sap_prelz_root: {attunity_sap_prelz_root}")

# COMMAND ----------

try:
    base_df = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", True)  # reprocess updates to old files, if any
        .load(f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}")
        .withColumn("__src_file", F.input_file_name())
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

#display(base_df)

# COMMAND ----------

try:
    ct_df = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", True)  # reprocess updates to old files, if any
        .load(f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}__ct")
        # Ignore "Before Image" records from update operations
        .filter("header__change_oper != 'B'")
        .withColumn("__src_file", F.input_file_name())
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

#display(ct_df)

# COMMAND ----------

clean_base_df = transform_utils.clean_column_names(dbutils=dbutils, df=base_df)
clean_ct_df = transform_utils.clean_column_names(dbutils=dbutils, df=ct_df)

#display(clean_base_df)
#display(clean_ct_df)

# COMMAND ----------

transformed_base_df = transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=clean_base_df)
transformed_ct_df = transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=clean_ct_df)

#display(transformed_base_df)
#display(transformed_ct_df)

# COMMAND ----------

union_df = transformed_base_df.unionByName(transformed_ct_df, allowMissingColumns=True)

#display(union_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=union_df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_bronze_table_location(
    dbutils=dbutils,
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"location: {location}")

# COMMAND ----------

try:
    # Default return object used when no new data is available
    results = common_utils.ReturnObject(
        status=common_utils.RunStatus.SUCCEEDED,
        target_object=f"{target_hive_database}.{target_hive_table}",
    )

    # Write streaming micro-batch capturing row statistics
    def append_to_bronze_table(df, _):
        global results
        results = write_utils.write_delta_table(
            spark=spark,
            df=df,
            location=location,
            database_name=target_hive_database,
            table_name=target_hive_table,
            load_type=write_utils.LoadType.APPEND_ALL,
            partition_columns=["TARGET_APPLY_DT"],
            schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
            enable_caching=False,
        )

    # Allow creation of delta table in a folder which is not empty
    # This is required because checkpoint folder will be there already
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

    # Trigger streaming micro-batch
    (
        audit_df.writeStream
        .option("checkpointLocation", location.rstrip("/") + "/_checkpoint")
        .trigger(once=True)
        .foreachBatch(append_to_bronze_table)
        .start()
        .awaitTermination()
    )

    print(results)

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
