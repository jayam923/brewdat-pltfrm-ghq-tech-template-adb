# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
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

dbutils.widgets.text("partition_column", "__ref_dt", "9 - partition_column")
partition_column = dbutils.widgets.get("partition_column")
partition_column = json.loads(partition_column)
print(f"partition_column: {partition_column}")

dbutils.widgets.text("raw_path", "data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/", "10 - raw_path")
raw_path = dbutils.widgets.get("raw_path")
print(f"raw_path: {raw_path}")

dbutils.widgets.text("watermark_column", "__ref_dt", "11 - watermark_column")
watermark_column = dbutils.widgets.get("watermark_column")
watermark_column = json.loads(watermark_column)
print(f"watermark_column: {watermark_column}")

dbutils.widgets.text("source_hive_database", "null", "12 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"source_hive_database: {source_hive_database}")

dbutils.widgets.text("source_hive_table", "null", "13 - source_hive_table")
source_hive_table = dbutils.widgets.get("source_hive_table")
print(f"source_hive_table: {source_hive_table}")

dbutils.widgets.text("key_column", "null", "14 - key_column")
key_column = dbutils.widgets.get("key_column")
key_column = json.loads(key_column)
print(f"key_column: {key_column}")

dbutils.widgets.text("silver_column_mapping", "[]", "15 - silver_column_mapping")
silver_column_mapping = dbutils.widgets.get("silver_column_mapping")
silver_column_mapping = json.loads(silver_column_mapping)
print(f"silver_column_mapping: {silver_column_mapping}")

dbutils.widgets.text("spark_sql_query", "null", "16 - spark_sql_query")
spark_sql_query = dbutils.widgets.get("spark_sql_query")
print(f"spark_sql_query: {spark_sql_query}")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, data_quality_utils, lakehouse_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
#help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    storage_account_names=[adls_raw_bronze_storage_account_name, adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

from pyspark.sql import functions as F

try:
    key_columns = [key_column]

    bronze_df = (
        spark.read
        .table(f"{source_hive_database}.{source_hive_table}")
        .filter(F.col(watermark_column).between(
            F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
            F.date_format(F.lit(data_interval_end), "yyyyMMdd")
        ))
    )

except Exception:
    common_utils.exit_with_last_exception()

#display(bronze_df)

# COMMAND ----------

# Apply data quality checks based on given column mappings
try:
    dq_checker = data_quality_utils.DataQualityChecker(bronze_df)
    mappings = [common_utils.ColumnMapping(**mapping) for mapping in silver_column_mapping]
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
    common_utils.exit_with_last_exception()
#display(bronze_dq_df)

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

dedup_df = transform_utils.deduplicate_records(
    df=transformed_df,
    key_columns=key_columns,
    watermark_column=watermark_column,
)

#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dedup_df)

#display(audit_df)

# COMMAND ----------

silver_dq_df = (
    data_quality_utils.DataQualityChecker(audit_df)
    .check_composite_column_value_is_unique(column_names=key_column)
    .check_column_value_is_not_in(column_name="StatusDescription", invalid_values=["--MAPPING ERROR--"])
    .check_narrow_condition(
        expected_condition="TotalDue - SubTotal - TaxAmt - Freight < 0.01",
        failure_message="CHECK_TOTAL_DUE: TotalDue should be equal to SubTotal + TaxAmt + Freight",
    )
    .build_df()
)

#display(silver_dq_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_silver_table_location(
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"target_location: {target_location}")

# COMMAND ----------

results = write_utils.write_delta_table(
    df=silver_dq_df,
    location=target_location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_column,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
