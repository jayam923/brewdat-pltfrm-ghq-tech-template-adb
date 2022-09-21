# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.5.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_system", "adventureworks", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_database", "slv_ghq_tech_adventureworks", "5 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "address", "6 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"{data_interval_end = }")

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
    storage_account_names=[adls_raw_bronze_storage_account_name, adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

from pyspark.sql import functions as F

try:
    key_columns = ["AddressID"]

    bronze_df = (
        spark.read
        .table("brz_ghq_tech_adventureworks.address")
        .filter(F.col("__ref_dt").between(
            F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
            F.date_format(F.lit(data_interval_end), "yyyyMMdd")
        ))
    )

except Exception:
    common_utils.exit_with_last_exception()

# display(bronze_df)

# COMMAND ----------

bronze_dq_df = (
    data_quality_utils.DataQualityChecker(bronze_df)
    .check_column_is_not_null(column_name="AddressID")
    .check_column_type_cast(column_name="AddressID", data_type="int")
    .check_column_type_cast(column_name="ModifiedDate", data_type="timestamp")
    .check_column_max_length(column_name="City", maximum_length=30)
    .check_column_max_length(column_name="StateProvince", maximum_length=50)
    .check_column_max_length(column_name="CountryRegion", maximum_length=50)
    .build_df()
)

bronze_dq_df.createOrReplaceTempView("v_bronze_dq_df")

# display(bronze_dq_df)

# COMMAND ----------

transformed_df = spark.sql("""
    SELECT
        CAST(AddressID AS INT) AS AddressID,
        City,
        StateProvince,
        CountryRegion,
        TO_TIMESTAMP(ModifiedDate) AS ModifiedDate,
        __data_quality_issues
    FROM
        v_bronze_dq_df
""")

# display(transformed_df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    df=transformed_df,
    key_columns=key_columns,
    watermark_column="ModifiedDate",
)

# display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dedup_df)

# display(audit_df)

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
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
