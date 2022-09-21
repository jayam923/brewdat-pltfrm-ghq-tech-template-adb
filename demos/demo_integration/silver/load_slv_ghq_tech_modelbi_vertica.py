# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "vertica", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_vertica", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "rebaunce", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

dbutils.widgets.text("raw_base_path", "", "9 - raw_base_path")
raw_base_path = dbutils.widgets.get("raw_base_path")
print(f"raw_base_path: {raw_base_path}")

dbutils.widgets.text("run_id", "", "10 - run_id")
run_id = dbutils.widgets.get("run_id")
print(f"run_id: {run_id}")

dbutils.widgets.text("source_hive_table", "", "10 - source_hive_table")
source_hive_table = dbutils.widgets.get("source_hive_table")
print(f"source_hive_table: {source_hive_table}")

# COMMAND ----------

import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# common_utils.configure_spn_access_for_adls(
#     spark=spark,
#     dbutils=dbutils,
#     storage_account_names=[adls_raw_bronze_storage_account_name_maz],
#     key_vault_name=key_vault_name_maz,
#     spn_client_id=spn_client_id_maz,
#     spn_secret_name=spn_secret_name_maz,
# )

# COMMAND ----------

source_location = lakehouse_bronze_root_maz + raw_base_path + source_hive_table
print(source_location)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.DELTA,
    location=source_location,
)

#display(raw_df)

# COMMAND ----------

clean_df = transform_utils.clean_column_names(dbutils=dbutils, df=raw_df)

#display(clean_df)

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = (
    clean_df
    .filter(F.col("__ref_dt").between(
        F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
        F.date_format(F.lit(data_interval_end), "yyyyMMdd"),
    ))
    .withColumn("__src_file", F.input_file_name())
    .withColumn("run_id", F.lit(run_id))
)

#display(transformed_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=transformed_df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root_maz,
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
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["__ref_dt","run_id"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    enable_caching=False,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
