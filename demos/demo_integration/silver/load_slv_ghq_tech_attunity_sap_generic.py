# Databricks notebook source
import json

dbutils.widgets.text("brewdat_library_version", "v0.5.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_system", "sap_europe", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("source_database", "brz_ghq_tech_sap_europe", "03 - source_database")
source_database = dbutils.widgets.get("source_database")
print(f"{source_database = }")

dbutils.widgets.text("source_table", "kna1", "04 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_zone", "ghq", "05 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "06 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_database", "slv_ghq_tech_sap_europe", "07 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "kna1", "08 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-08-02 00:00:00.0000000", "09 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text("column_mapping", "[]", "10 - column_mapping")
column_mapping = dbutils.widgets.get("column_mapping")
column_mapping = json.loads(column_mapping)
print(f"{column_mapping = }")

dbutils.widgets.text("key_columns", '["MANDT", "KUNNR"]', "11 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
key_columns = json.loads(key_columns)
print(f"{key_columns = }")

dbutils.widgets.text("partition_columns", "[]", "12 - partition_columns")
partition_columns = dbutils.widgets.get("partition_columns")
partition_columns = json.loads(partition_columns)
print(f"{partition_columns = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, data_quality_utils, lakehouse_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(transform_utils)

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

try:
    latest_partition = (
        spark.read
        .table(f"{source_database}.{source_table}")
        .agg(F.max("TARGET_APPLY_DT"))
        .collect()[0][0]
    )
    print(f"{latest_partition = }")

    max_watermark_value = (
        spark.read
        .table(f"{source_database}.{source_table}")
        .filter(F.col("TARGET_APPLY_DT") == F.lit(latest_partition))
        .agg(F.max("TARGET_APPLY_TS"))
        .collect()[0][0]
    )
    print(f"{max_watermark_value = }")

    effective_data_interval_end = max_watermark_value
    print(f"{effective_data_interval_end = }")

except Exception:
    common_utils.exit_with_last_exception()

# COMMAND ----------

try:
    bronze_df = (
        spark.read
        .table(f"{source_database}.{source_table}")
        .filter(F.col("TARGET_APPLY_DT").between(
            F.to_date(F.lit(data_interval_start)),
            F.to_date(F.lit(effective_data_interval_end)),
        ))
        .filter(F.col("TARGET_APPLY_TS").between(
            F.to_timestamp(F.lit(data_interval_start)),
            F.to_timestamp(F.lit(effective_data_interval_end)),
        ))
        # Ignore "Before Image" records from update operations
        .filter("header__change_oper != 'B'")
    )

except Exception:
    common_utils.exit_with_last_exception()

# display(bronze_df)

# COMMAND ----------

try:
    # Apply data quality checks based on given column mappings
    dq_checker = data_quality_utils.DataQualityChecker(bronze_df)
    mappings = [common_utils.ColumnMapping(**mapping) for mapping in column_mapping]
    for mapping in mappings:
        if mapping.target_data_type != "string":
            dq_checker = dq_checker.check_column_type_cast(
                column_name=mapping.source_column_name,
                data_type=mapping.target_data_type,
            )
        if not mapping.nullable:
            dq_checker = dq_checker.check_column_is_not_null(mapping.source_column_name)

    bronze_dq_df = dq_checker.build_df()

    # display(bronze_dq_df)

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

# display(transformed_df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    df=transformed_df,
    key_columns=key_columns,
    watermark_column="SOURCE_COMMIT_TS",
)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dedup_df)

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

results = write_utils.write_delta_table(
    df=audit_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns,
    partition_columns=partition_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode=write_utils.BadRecordHandlingMode.REJECT,
    update_condition_for_upsert="source.SOURCE_COMMIT_TS >= target.SOURCE_COMMIT_TS",
)

results.effective_data_interval_start = data_interval_start
results.effective_data_interval_end = effective_data_interval_end or data_interval_start

# Warn in case of relevant unmapped columns
unmapped_columns = list(filter(lambda c: not c.startswith("header__"), unmapped_columns))
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
