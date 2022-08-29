# Databricks notebook source
import json
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

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_adventureworks", "5 - target_hive_database")
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

dbutils.widgets.text("json_dq_wide_mapping", "[]", "9 - json_dq_wide_mapping")
json_dq_wide_mapping = dbutils.widgets.get("json_dq_wide_mapping")
json_dq_wide_mapping = json.loads(json_dq_wide_mapping)
print(f"json_dq_wide_mapping: {json_dq_wide_mapping}")

dbutils.widgets.text("key_columns", '["MANDT", "KUNNR"]', "10 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
key_columns = json.loads(key_columns)
print(f"key_columns: {key_columns}")

dbutils.widgets.text("compond_column_unique_percentage", "0.5", "11 - compond_column_unique_percentage")
compond_column_unique_percentage = float(dbutils.widgets.get("compond_column_unique_percentage"))
print(f"compond_column_unique_percentage: {compond_column_unique_percentage}")

dbutils.widgets.text("count_variation_with_prev_min_value", "100", "12 - count_variation_with_prev_min_value")
count_variation_with_prev_min_value = int(dbutils.widgets.get("count_variation_with_prev_min_value"))
print(f"count_variation_with_prev_min_value: {count_variation_with_prev_min_value}")

dbutils.widgets.text("count_variation_with_prev_max_value", "200", "13 - count_variation_with_prev_max_value")
count_variation_with_prev_max_value = int(dbutils.widgets.get("count_variation_with_prev_max_value"))
print(f"count_variation_with_prev_max_value: {count_variation_with_prev_max_value}")

dbutils.widgets.text("row_count_min_value", "100", "14 - row_count_min_value")
row_count_min_value = int(dbutils.widgets.get("row_count_min_value"))
print(f"row_count_min_value: {row_count_min_value}")

dbutils.widgets.text("row_count_max_value", "200", "15 - row_count_max_value")
row_count_max_value = int(dbutils.widgets.get("row_count_max_value"))
print(f"row_count_max_value: {row_count_max_value}")



# COMMAND ----------

import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils, data_quality_utils, data_quality_wider_check

# Print a module's help
help(data_quality_wider_check)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = (
    spark.read
    .table(f"{target_hive_database}.{target_hive_table}")
    .filter(F.col("__ref_dt").between(
        F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
        F.date_format(F.lit(data_interval_end), "yyyyMMdd"),
    ))
    .withColumn("__src_file", F.input_file_name())
).limit(2)

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=transformed_df)
display(audit_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    dbutils=dbutils,
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)

results = write_utils.write_delta_table(
    spark=spark,
    df= audit_df,
    location=target_location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["__ref_dt"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(vars(results))



# COMMAND ----------

data_quality_wider_modify=data_quality_wider_check.DataQualityCheck(df=audit_df,dbutils=dbutils,spark=spark)

# COMMAND ----------

mappings = [common_utils.widerColumnMapping(**mapping) for mapping in  json_dq_wide_mapping]
for mapping in mappings:
    if mapping.unique_percentage_col is not None:
        data_quality_wider_modify.dq_validate_column_unique_values(col_name = mapping.source_column_name, mostly = mapping.unique_percentage_col)
    if mapping.null_percentage_for_col is not None:
        data_quality_wider_modify.dq_validate_column_values_to_not_be_null( col_name = mapping.source_column_name, mostly =mapping.null_percentage_for_col)
    if mapping.null_percentage_variation_with_prev is not None:
        data_quality_wider_modify.dq_validate_null_percentage_variation_from_previous_version_values(target_location = target_location,  col_name = mapping.source_column_name, mostly = mapping.null_percentage_variation_with_prev,older_version=results.old_version_number,latest_version=results.new_version_number)
    
if key_columns is not None:
    data_quality_wider_modify.dq_validate_compond_column_unique_values(col_list = key_columns , mostly =compond_column_unique_percentage)
if (count_variation_with_prev_min_value is not None) and (count_variation_with_prev_max_value is not None):
    data_quality_wider_modify.dq_validate_count_variation_from_previous_version_values( target_location = target_location, min_value = count_variation_with_prev_min_value, max_value = count_variation_with_prev_max_value,older_version=results.old_version_number,latest_version=results.new_version_number)
if (row_count_min_value is not None) and (row_count_max_value is not None):
    data_quality_wider_modify.dq_validate_row_count( min_value =row_count_min_value, max_value = row_count_min_value)

        
final_result_df = data_quality_wider_modify.get_wider_dq_results(spark=spark, dbutils=dbutils)
display(final_result_df)

