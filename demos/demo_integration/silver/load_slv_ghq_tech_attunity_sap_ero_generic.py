# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.3.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "attunity_sap_ero", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "03 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "04 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_attunity_sap_ero", "05 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "kna1", "06 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("source_hive_database", "brz_ghq_tech_attunity_sap_ero", "07 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"source_hive_database: {source_hive_database}")

dbutils.widgets.text("data_interval_start", "2022-08-02 00:00:00.0000000", "08 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", "", "09 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

dbutils.widgets.text("silver_schema", "", "10 - silver_schema")
silver_schema = dbutils.widgets.get("silver_schema")
print(f"silver_schema: {silver_schema}")

dbutils.widgets.text("key_columns", "MANDT,KUNNR", "11 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
print(f"key_columns: {key_columns}")

# COMMAND ----------

import json
import sys
import pyspark.sql.functions as F
import datetime

silver_schema = json.loads(silver_schema)
key_columns_list = key_columns.split(",")

# COMMAND ----------

# Import BrewDat Library modules
#sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
sys.path.append(f"/Workspace/Repos/sachin.kumar@ab-inbev.com/brewdat-pltfrm-ghq-tech-template-adb")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

# COMMAND ----------

target_schema = []
for row in silver_schema:
    target_schema.append(common_utils.RowSchema(row["source_attribute_name"], row["target_attribute_name"],  row["target_data_type"]))

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

brz_df = spark.sql(f"select * from {source_hive_database}.{target_hive_table} where TARGET_APPLY_DT >= TO_DATE('{data_interval_start}')")
if not data_interval_end:
    watermark_upper_bound = brz_df.select(F.max(F.to_timestamp("TARGET_APPLY_TS"))).collect()[0][0]
    data_interval_end = watermark_upper_bound.strftime("%Y-%m-%d %H:%M:%S.%f")

filtered_df = brz_df.filter(F.col("TARGET_APPLY_TS").between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(data_interval_end)),
    ))
print(data_interval_end)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=filtered_df,
    key_columns=key_columns_list,
    watermark_column="SOURCE_COMMIT_TS",
)

modified_df, error_message = transform_utils.apply_schema(dbutils=dbutils, df=dedup_df, schema=target_schema)
#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=modified_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(location)

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns_list,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    update_condition="source.SOURCE_COMMIT_TS > target.SOURCE_COMMIT_TS",
)

results.data_interval_start = data_interval_start
results.data_interval_end = data_interval_end
results.error_message = error_message

print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
