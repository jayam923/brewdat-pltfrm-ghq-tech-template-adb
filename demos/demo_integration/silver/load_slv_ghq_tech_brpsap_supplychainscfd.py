# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.3.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "brp_sap", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_brpsap", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "supplychain_scfd", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "0001-01-01T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", "2022-08-04T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

# COMMAND ----------

import os
import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
# help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[adls_raw_bronze_storage_account_name, adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

key_columns = ["ID"]

df = spark.sql("""
        SELECT
            CAST(ID AS INT) AS id,
            DATA_SUBJECT_CD AS data_subject_cd,
            SOURCE_APP_NAME AS source_app_name,
            CAST(SOURCE_SYSTEM_ID AS INT) AS source_system_id,
            SOURCE_REGION_NAME AS source_region_name,
            SOURCE_REGION_TYPE AS source_region_type,
            LOAD_TYPE AS load_type,
            TABLE_NAME AS table_name,
            TO_DATE(START_DATE) AS start_date,
            TO_DATE(END_DATE) AS end_date,
            STATUS AS status,
            __update_gmt_ts AS __raw_ingestion_timestamp
        FROM
            brz_ghq_tech_brpsap.supplychain_scfd
        WHERE 1 = 1
            AND __ref_dt BETWEEN DATE_FORMAT('{data_interval_start}', 'yyyyMMdd') AND DATE_FORMAT('{data_interval_end}', 'yyyyMMdd')
            AND __update_gmt_ts BETWEEN '{data_interval_start}' AND '{data_interval_end}'
    """.format(
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    ))

#display(df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=df,
    key_columns=key_columns,
    watermark_column="__raw_ingestion_timestamp",
).drop("__raw_ingestion_timestamp")

display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
