# Databricks notebook source
import json

dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_system", "sap_ecc_bt1", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("source_database", "brz_afr_tech_sap_ecc_bt1", "03 - source_database")
source_database = dbutils.widgets.get("source_database")
print(f"{source_database = }")

dbutils.widgets.text("source_table", "afih", "04 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_zone", "afr", "05 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "06 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_database", "slv_afr_tech_sap_ecc_bt1", "07 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "afih", "08 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("column_mapping", "[]", "09 - column_mapping")
column_mapping = dbutils.widgets.get("column_mapping")
column_mapping = json.loads(column_mapping)
print(f"{column_mapping = }")

dbutils.widgets.text("key_columns", '["MANDT", "AUFNR"]', "10 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
key_columns = json.loads(key_columns)
print(f"{key_columns = }")

dbutils.widgets.text("partition_columns", "[]", "11 - partition_columns")
partition_columns = dbutils.widgets.get("partition_columns")
partition_columns = json.loads(partition_columns)
print(f"{partition_columns = }")

dbutils.widgets.text("load_type", "UPSERT", "11 - load_type")
load_type = dbutils.widgets.get("load_type")
print(f"{load_type = }")

dbutils.widgets.text("reset_stream_checkpoint", "false", "12 - reset_stream_checkpoint")
reset_stream_checkpoint = dbutils.widgets.get("reset_stream_checkpoint")
print(f"{reset_stream_checkpoint = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import read_utils, common_utils, data_quality_utils, lakehouse_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
#help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    storage_account_names=[
        adls_raw_bronze_storage_account_name,
        adls_silver_gold_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = (
    read_utils.read_raw_streaming_dataframe(
        file_format=read_utils.RawFileFormat.DELTA,
        table_name=f"{source_database}.{source_table}",      
    )
)

# COMMAND ----------

try:
    # Apply data quality checks based on given column mappings
    dq_checker = data_quality_utils.DataQualityChecker(bronze_df)
    mappings = [common_utils.ColumnMapping(**mapping) for mapping in column_mapping]
    for mapping in mappings:
        dq_checker = dq_checker.check_column_type_cast(
                column_name=mapping.source_column_name,
                data_type=mapping.target_data_type,
            )   
        if not mapping.nullable:
            dq_checker = dq_checker.check_column_is_not_null(mapping.source_column_name)

    bronze_dq_df = dq_checker.build_df()

    #display(bronze_dq_df)

except Exception:
    common_utils.exit_with_last_exception()

# COMMAND ----------

# Preserve data quality results
dq_results_column = common_utils.ColumnMapping(
    source_column_name=data_quality_utils.DQ_RESULTS_COLUMN,
    target_data_type="array<string>",
)
mappings.append(dq_results_column)

# Apply column mappings and retrieve list of unmapped columns
transformed_df, unmapped_columns = transform_utils.apply_column_mappings(df=bronze_dq_df, mappings=mappings)

#display(transformed_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(transformed_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_silver_table_location(
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_table,
)
print(f"{target_location = }")

# COMMAND ----------

def deduplicate(df):
    return  transform_utils.deduplicate_records(df=df, key_columns=key_columns, watermark_column="AEDATTM")

results = write_utils.write_stream_delta_table(
    df=audit_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=write_utils.LoadType[load_type],
    key_columns=key_columns,
    partition_columns=partition_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode=write_utils.BadRecordHandlingMode.REJECT,
    auto_broadcast_join_threshold=10485760,
    transform_microbatch=deduplicate,
    reset_checkpoint=(reset_stream_checkpoint.lower() == "true"),
)

# Warn in case of relevant unmapped columns
if unmapped_columns:
    formatted_columns = ", ".join(f"`{col}`" for col in unmapped_columns)
    unmapped_warning = "WARNING: the following columns are not mapped: " + formatted_columns
    if results.error_message:
        results.error_message += "; "
    results.error_message += unmapped_warning
    if results.error_details:
        results.error_details += "; "
    results.error_details += unmapped_warning

print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
