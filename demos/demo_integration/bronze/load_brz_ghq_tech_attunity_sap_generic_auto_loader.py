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

dbutils.widgets.text("reset_checkpoint", "false", "07 - reset_checkpoint")
reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
print(f"reset_checkpoint: {reset_checkpoint}")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules
#sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
sys.path.append(f"/Workspace/Repos/Repos/sachin.kumar@ab-inbev.com/autoloader/brewdat_library")

from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

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
raw_location = f"{lakehouse_raw_root}/data/{target_zone}/{target_business_domain}/sap_{sap_sid}/file.{source_table}"
print(f"raw_location: {raw_location}")

# COMMAND ----------

location = lakehouse_utils.generate_bronze_table_location(
    dbutils=dbutils,
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)

# COMMAND ----------

base_df = (
    read_utils.read_raw_dataframe_stream(
        spark=spark,
        dbutils=dbutils,
        file_format = read_utils.RawFileFormat.PARQUET,
        location = raw_location,
        cast_all_to_string = True,
        use_incremental_listing = False,
        allow_overwrites=True,
        rescue_columns=True,
    )
    .withColumn("__src_file", F.input_file_name())
)

# COMMAND ----------

ct_df = (
    read_utils.read_raw_dataframe_stream(
        spark=spark,
        dbutils=dbutils,
        file_format = read_utils.RawFileFormat.PARQUET,
        location = f"{raw_location}__ct",
        cast_all_to_string = True,
        use_incremental_listing = True,
        allow_overwrites=True,
        rescue_columns=True,
    )
    .withColumn("__src_file", F.input_file_name())
)

# COMMAND ----------

clean_base_df = transform_utils.clean_column_names(dbutils=dbutils, df=base_df)
clean_ct_df = transform_utils.clean_column_names(dbutils=dbutils, df=ct_df)

# COMMAND ----------

transformed_base_df = transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=clean_base_df)
transformed_ct_df = transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=clean_ct_df)

# COMMAND ----------

union_df = transformed_base_df.unionByName(transformed_ct_df, allowMissingColumns=True)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=union_df)

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

result = write_utils.write_stream_delta_table(
    dbutils=dbutils,
    spark=spark,
    df=audit_df,
    location=location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode=write_utils.BadRecordHandlingMode.WARN,
    enable_caching=False,
    reset_checkpoint=True if reset_checkpoint.lower() == "true" else False
)

print(result)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=result)
