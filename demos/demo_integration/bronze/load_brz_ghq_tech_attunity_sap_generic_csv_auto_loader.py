# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_zone", "ghq", "02 - source_zone")
source_zone = dbutils.widgets.get("source_zone")
print(f"{source_zone = }")

dbutils.widgets.text("source_business_domain", "tech", "03 - source_business_domain")
source_business_domain = dbutils.widgets.get("source_business_domain")
print(f"{source_business_domain = }")

dbutils.widgets.text("source_system", "sap_europe", "04 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("source_table", "KNA1", "05 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_sap_europe", "06 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"{target_hive_database = }")

dbutils.widgets.text("target_hive_table", "kna1", "07 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"{target_hive_table = }")

dbutils.widgets.text("reset_checkpoint", "false", "08 - reset_checkpoint")
reset_checkpoint = dbutils.widgets.get("reset_checkpoint")
print(f"{reset_checkpoint = }")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
#help(read_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
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
raw_location = f"{lakehouse_raw_root}/data/{source_zone}/{source_business_domain}/{sap_sid}/{source_table}"
print(f"{raw_location = }")

# COMMAND ----------

base_df = (
    read_utils.read_raw_streaming_dataframe(
        file_format=read_utils.RawFileFormat.CSV,
        location=f"{raw_location}/*.csv*",
        schema_location=raw_location,
        csv_delimiter="|~|",
        additional_options={
            "cloudFiles.useIncrementalListing": "false",
        },
    )
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.create_or_replace_audit_columns)
)

#display(base_df)

# COMMAND ----------

ct_df = (
    read_utils.read_raw_streaming_dataframe(
        file_format=read_utils.RawFileFormat.CSV,
        location=f"{raw_location}__ct/*.csv*",
        schema_location=f"{raw_location}__ct",
        csv_delimiter="|~|",
    )
    # Ignore "Before Image" records from update operations
    .filter("header__change_oper != 'B'")
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.create_or_replace_audit_columns)
)

#display(ct_df)

# COMMAND ----------

union_df = base_df.unionByName(ct_df, allowMissingColumns=True)

#display(union_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=source_zone,
    target_business_domain=source_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_stream_delta_table(
    df=union_df,
    location=target_location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode=write_utils.BadRecordHandlingMode.WARN,
    enable_caching=False,
    reset_checkpoint=(reset_checkpoint.lower() == "true"),
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)