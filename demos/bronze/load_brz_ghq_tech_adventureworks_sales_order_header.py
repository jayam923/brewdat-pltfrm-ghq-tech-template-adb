# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.2.0", "1 - brewdat_library_version")
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

# COMMAND ----------

import os
import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

# COMMAND ----------

# Gather standard Lakehouse environment variables
environment = os.getenv("ENVIRONMENT")
lakehouse_raw_root = os.getenv("LAKEHOUSE_RAW_ROOT")
lakehouse_bronze_root = os.getenv("LAKEHOUSE_BRONZE_ROOT")

# Ensure that all standard Lakehouse environment variables are set
if None in [environment, lakehouse_raw_root, lakehouse_bronze_root]:
    raise Exception("This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster.")

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
if environment == "dev":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmrawbrzd"],
        key_vault_name="brewdatpltfrmghqtechakvd",
        spn_client_id="1d3aebfe-929c-4cc1-a988-31c040d2b798",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-d",
    )
elif environment == "qa":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmrawbrzq"],
        key_vault_name="brewdatpltfrmghqtechakvq",
        spn_client_id="12345678-1234-1234-1234-123456789999",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-q",
    )
elif environment == "prod":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmrawbrzp"],
        key_vault_name="brewdatpltfrmghqtechakvp",
        spn_client_id="12345678-1234-1234-1234-123456789999",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-p",
    )

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/csv/",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
    excel_sheet_name = None
)
display(raw_df)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.AVRO,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/avro/",
    excel_sheet_name = None
)
display(raw_df)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.JSON,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/json/",
    excel_sheet_name = None
)
display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, input_file_name
raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.XML,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/xml/*/*.xml",
    xml_row_tag = "catalog_item",
    excel_sheet_name = None,
)
raw_df = raw_df.withColumn("__ref_dt",regexp_extract(input_file_name(),'[0-9]+',0))
display(raw_df)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.EXCEL,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/excel/",
    excel_sheet_name = "testing",
)
raw_df = raw_df.withColumn("__ref_dt",regexp_extract(input_file_name(),'[0-9]+',0))
display(raw_df)

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
    partition_columns=["__ref_dt"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
