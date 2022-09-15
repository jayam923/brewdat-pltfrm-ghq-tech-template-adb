# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_system", "sap_europe", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("source_table", "KNA1", "03 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_zone", "ghq", "04 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "05 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_sap_europe", "06 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"{target_hive_database = }")

dbutils.widgets.text("target_hive_table", "kna1", "07 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"{target_hive_table = }")

dbutils.widgets.text("data_interval_start", "2022-08-05 00:00:00.000000", "08 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
#help(read_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    storage_account_names=[
        adls_raw_bronze_storage_account_name,
        adls_brewdat_ghq_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

sap_sid = source_system.split('_',2)[2]
attunity_sap_prelz_root = f"/attunity_sap/attunity_sap_{sap_sid}_prelz/prelz_sap_{sap_sid}"
print(f"{attunity_sap_prelz_root = }")

# COMMAND ----------

base_df = (
    read_utils.read_raw_dataframe(
        file_format=read_utils.RawFileFormat.DELTA,
        location=f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}",
    )
    .filter(F.col("TARGET_APPLY_DT") >= F.to_date(F.lit(data_interval_start)))
)

#display(base_df)

# COMMAND ----------

ct_df = (
    read_utils.read_raw_dataframe(
        file_format=read_utils.RawFileFormat.DELTA,
        location=f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}__ct",
    )
    .filter(F.col("TARGET_APPLY_DT") >= F.to_date(F.lit(data_interval_start)))
)

#display(ct_df)

# COMMAND ----------

max_base_watermark_value = base_df.select(F.max(F.col("TARGET_APPLY_TS")).cast("string")).collect()[0][0]
print(f"{max_base_watermark_value = }")

max_ct_watermark_value = ct_df.select(F.max(F.col("TARGET_APPLY_TS")).cast("string")).collect()[0][0]
print(f"{max_ct_watermark_value = }")

effective_data_interval_end = max_ct_watermark_value or max_base_watermark_value
print(f"{effective_data_interval_end = }")

# COMMAND ----------

transformed_base_df = (
    base_df
    .filter(F.col("TARGET_APPLY_TS").between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(effective_data_interval_end)),
    ))
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.cast_all_columns_to_string)
    .transform(transform_utils.create_or_replace_audit_columns)
)

#display(transformed_base_df)

# COMMAND ----------

transformed_ct_df = (
    ct_df
    .filter(F.col("TARGET_APPLY_TS").between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(effective_data_interval_end)),
    ))
    # Ignore "Before Image" records from update operations
    .filter("header__change_oper != 'B'")
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.cast_all_columns_to_string)
    .transform(transform_utils.create_or_replace_audit_columns)
)

#display(transformed_ct_df)

# COMMAND ----------

union_df = transformed_base_df.unionByName(transformed_ct_df, allowMissingColumns=True)

#display(union_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_delta_table(
    df=union_df,
    location=target_location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    enable_caching=False,
)
results.effective_data_interval_start = data_interval_start
results.effective_data_interval_end = effective_data_interval_end or data_interval_start
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
