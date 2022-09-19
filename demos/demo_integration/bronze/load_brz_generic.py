# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.5.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("source_system", "adventureworks", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text(
    "source_location",
    "data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/",
    "03 - source_location",
)
source_location = dbutils.widgets.get("source_location")
print(f"{source_location = }")

dbutils.widgets.text("partition_columns", '["__ref_dt"]', "04 - partition_columns")
partition_columns = dbutils.widgets.get("partition_columns")
print(f"{partition_columns = }")

dbutils.widgets.text("target_zone", "ghq", "05 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "06 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_database", "brz_ghq_tech_adventureworks", "07 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "sales_order_header", "08 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "09 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "10 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"{data_interval_end = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(read_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    storage_account_names=[adls_raw_bronze_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    file_format=read_utils.RawFileFormat.ORC,
    location=f"{lakehouse_raw_root}/{source_location}",
)

# display(raw_df)

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = (
    raw_df
    .filter(F.col("__ref_dt").between(
        F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
        F.date_format(F.lit(data_interval_end), "yyyyMMdd"),
    ))
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.cast_all_columns_to_string)
    .transform(transform_utils.create_or_replace_audit_columns)
)

# display(transformed_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_table,
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_delta_table(
    df=transformed_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=partition_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    enable_caching=False,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
