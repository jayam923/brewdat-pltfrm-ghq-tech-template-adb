# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.3.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "attunity_sap_ero", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_attunity_sap_ero", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "bkpf", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-06-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("prelz_path", "/attunity_sap/attunity_sap_ero_prelz/prelz_sap_ero_KNA1", "8 - prelz_path")
prelz_path = dbutils.widgets.get("prelz_path")
print(f"prelz_path: {prelz_path}")

# COMMAND ----------

watermark_column = "TARGET_APPLY_TS"

# COMMAND ----------

import sys
from pyspark.sql import functions as F
# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
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
        adls_silver_gold_storage_account_name,
        adls_brewdat_ghq_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

base_raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.DELTA,
    location=f"{attunity_sap_root}/{prelz_path}",
    cast_all_to_string=False,
)

#display(base_raw_df)

# COMMAND ----------

ct_raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.DELTA,
    location=f"{attunity_sap_root}/{prelz_path}__ct",
    cast_all_to_string=False,
)

#display(ct_raw_df)

# COMMAND ----------

clean_base_df = transform_utils.clean_column_names(dbutils=dbutils, df=base_raw_df)
clean_ct_df = transform_utils.clean_column_names(dbutils=dbutils, df=ct_raw_df)

# COMMAND ----------

max_base_watermark_value = clean_base_df.select(F.max(F.col(watermark_column))).collect()[0][0]
max_ct_watermark_value = clean_ct_df.select(F.max(F.col(watermark_column))).collect()[0][0]
new_upper_bound = max_ct_watermark_value if max_ct_watermark_value > max_base_watermark_value else max_base_watermark_value
data_interval_end = new_upper_bound.strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

print(max_base_watermark_value, max_ct_watermark_value, new_upper_bound, data_interval_end)

# COMMAND ----------

filtered_base_df = (
    clean_base_df
    .filter(F.col(watermark_column).between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(max_base_watermark_value)),
    ))
)

#display(filtered_base_df)

# COMMAND ----------

filtered_ct_df = (
    clean_ct_df
    .filter(F.col(watermark_column).between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(max_ct_watermark_value)),
    ))
    .filter("header__change_oper != 'B'")
)

#display(filtered_ct_df)

# COMMAND ----------

union_df = filtered_base_df.unionByName(filtered_ct_df, allowMissingColumns=True)

# COMMAND ----------

transformed_df = (
    transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=union_df)
    .withColumn("__src_file", F.input_file_name())
)

#display(transformed_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=transformed_df)

#display(audit_df)

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
    df=audit_df,
    location=target_location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS
)
vars(results)["data_interval_end"] = data_interval_end
print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select  * from brz_ghq_tech_attunity_sap_ero.kna1
