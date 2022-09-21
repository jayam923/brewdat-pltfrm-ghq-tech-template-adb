# Databricks notebook source
import json

dbutils.widgets.text("brewdat_library_version", "v0.5.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("sql_query", "null", "02 - sql_query")
sql_query = dbutils.widgets.get("sql_query")
print(f"{sql_query = }")

dbutils.widgets.text("key_columns", "[]", "03 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
key_columns = json.loads(key_columns)
print(f"{key_columns = }")

dbutils.widgets.text("watermark_column", "__ref_dt", "04 - watermark_column")
watermark_column = dbutils.widgets.get("watermark_column")
print(f"{watermark_column = }")

dbutils.widgets.text("partition_columns", "__ref_dt", "05 - partition_columns")
partition_columns = dbutils.widgets.get("partition_columns")
partition_columns = json.loads(partition_columns)
print(f"{partition_columns = }")

dbutils.widgets.text("load_type", "OVERWRITE_TABLE", "06 - load_type")
load_type = dbutils.widgets.get("load_type")
print(f"{load_type = }")

dbutils.widgets.text("target_zone", "ghq", "07 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "08 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_data_product", "demo_consumption", "09 - target_data_product")
target_data_product = dbutils.widgets.get("target_data_product")
print(f"{target_data_product = }")

dbutils.widgets.text("target_database", "gld_ghq_tech_demo_consumption", "10 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "customer_orders", "11 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "12 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "13 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"{data_interval_end = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    storage_account_names=[adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

try:
    df = spark.sql(sql_query.format(
        watermark_column=watermark_column,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    ))

except Exception:
    common_utils.exit_with_last_exception()

# display(df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    df=df,
    key_columns=key_columns,
    watermark_column=watermark_column,
)

# display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dedup_df)

# display(audit_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_gold_table_location(
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    target_data_product=target_data_product,
    database_name=target_database,
    table_name=target_table,
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_delta_table(
    df=audit_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=load_type,
    key_columns=key_columns,
    partition_columns=partition_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
