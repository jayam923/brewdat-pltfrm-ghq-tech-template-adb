# Databricks notebook source
import json

dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "sap_europe", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("source_hive_database", "brz_ghq_tech_sap_europe", "03 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"source_hive_database: {source_hive_database}")

dbutils.widgets.text("source_hive_table", "kna1", "04 - source_hive_table")
source_hive_table = dbutils.widgets.get("source_hive_table")
print(f"source_hive_table: {source_hive_table}")

dbutils.widgets.text("target_zone", "ghq", "05 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "06 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_sap_europe", "07 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "kna1", "08 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-08-02 00:00:00.0000000", "09 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("silver_mapping", "[]", "10 - silver_mapping")
silver_mapping = dbutils.widgets.get("silver_mapping")
silver_mapping = json.loads(silver_mapping)
print(f"silver_mapping: {silver_mapping}")

dbutils.widgets.text("key_columns", '["MANDT", "KUNNR"]', "11 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
key_columns = json.loads(key_columns)
print(f"key_columns: {key_columns}")

dbutils.widgets.text("partition_columns", "[]", "12 - partition_columns")
partition_columns = dbutils.widgets.get("partition_columns")
partition_columns = json.loads(partition_columns)
print(f"partition_columns: {partition_columns}")

# COMMAND ----------

import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, data_quality_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
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
        .table(f"{source_hive_database}.{source_hive_table}")
        .agg(F.max("TARGET_APPLY_DT"))
        .collect()[0][0]
    )
    print(f"latest_partition: {latest_partition}")

    max_watermark_value = (
        spark.read
        .table(f"{source_hive_database}.{source_hive_table}")
        .filter(F.col("TARGET_APPLY_DT") == F.lit(latest_partition))
        .agg(F.max("TARGET_APPLY_TS"))
        .collect()[0][0]
    )
    print(f"max_watermark_value: {max_watermark_value}")

    effective_data_interval_end = max_watermark_value
    print(f"effective_data_interval_end: {effective_data_interval_end}")

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

try:
    bronze_df = (
        spark.read
        .table(f"{source_hive_database}.{source_hive_table}")
        .filter(F.col("TARGET_APPLY_DT").between(
            F.to_date(F.lit(data_interval_start)),
            F.to_date(F.lit(effective_data_interval_end)),
        ))
        .filter(F.col("TARGET_APPLY_TS").between(
            F.to_timestamp(F.lit(data_interval_start)),
            F.to_timestamp(F.lit(effective_data_interval_end)),
        ))
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

#display(bronze_df)

# COMMAND ----------

try:
    # Apply data quality checks based on given column mappings
    dq_checker = data_quality_utils.DataQualityChecker(dbutils=dbutils, df=bronze_df)
    mappings = [common_utils.ColumnMapping(**mapping) for mapping in silver_mapping]
    for mapping in mappings:
        if mapping.target_data_type != "string":
            dq_checker = dq_checker.check_column_type_cast(
                column_name=mapping.source_column_name,
                data_type=mapping.target_data_type,
            )
        if not mapping.nullable:
            dq_checker = dq_checker.check_column_is_not_null(mapping.source_column_name)

    bronze_dq_df = dq_checker.build_df()

    #display(bronze_dq_df)

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

# COMMAND ----------

# Preserve data quality results
dq_results_column = common_utils.ColumnMapping(
    source_column_name=data_quality_utils.DQ_RESULTS_COLUMN,
    target_data_type="array<string>",
)
mappings.append(dq_results_column)

# Apply column mappings and retrieve list of unmapped columns
transformed_df, unmapped_columns = transform_utils.apply_column_mappings(dbutils=dbutils, df=bronze_dq_df, mappings=mappings)

#display(transformed_df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=transformed_df,
    key_columns=key_columns,
    watermark_column="SOURCE_COMMIT_TS",
)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"location: {location}")

# COMMAND ----------

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns,
    partition_columns=partition_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode=write_utils.BadRecordHandlingMode.REJECT,
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

common_utils.exit_with_object(dbutils=dbutils, results=results)
