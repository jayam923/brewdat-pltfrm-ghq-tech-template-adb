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

dbutils.widgets.text("target_country", "us", "4 - target_country")
target_country = dbutils.widgets.get("target_country")
print(f"target_country: {target_country}")

dbutils.widgets.text("target_business_domain", "tech", "5 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_attunity_sap_ero", "6 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "bkpf", "7 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-06-21T00:00:00Z", "8 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("incremental_load", "false", "10 - incremental_load")
incremental_load = dbutils.widgets.get("incremental_load")
print(f"incremental_load: {incremental_load}")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules
#sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
sys.path.append(f"/Workspace/Repos/sachin.kumar@ab-inbev.com/brewdat-pltfrm-ghq-tech-template-adb")
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

import datetime

# COMMAND ----------

base_table_path=f"{lakehouse_raw_root}/data/{target_zone}/{target_business_domain}/{source_system}/file.{target_hive_table}"

# COMMAND ----------

raw_locations = []
if incremental_load.lower() == 'true':
    #build list of locations
    last_partition_end_time = datetime.datetime.strptime(data_interval_start, "%Y-%m-%dT%H:%M:%SZ")
    ct_table_path = f"{base_table_path}__ct"
    partitions_to_load = read_utils.get_partitions_to_process(dbutils, ct_table_path, last_partition_end_time)
    if partitions_to_load:
        data_interval_start = partitions_to_load[0][1].strftime("%Y-%m-%dT%H:%M:%SZ")
        data_interval_end = partitions_to_load[-1][2].strftime("%Y-%m-%dT%H:%M:%SZ")
        raw_locations = [f"{table_path}/{partition[0]}/*.csv.gz" for partition in partitions_to_load]
    else:
        results = common_utils.ReturnObject(
            status=common_utils.RunStatus.SUCCEEDED,
            target_object=f"{target_hive_database}.{target_hive_table}",
            num_records_read=0,
            num_records_loaded=0,
            error_message='There are no new data in change table to process',
            error_details='')
        vars(results)["data_interval_start"] = data_interval_start
        vars(results)["data_interval_end"] = data_interval_start
        common_utils.exit_with_object(dbutils=dbutils, results=results)
        
        
else:
    data_interval_start=None
    data_interval_end = None
    raw_locations.append(f"{base_table_path}/*.csv.gz")
    

# COMMAND ----------

print(data_interval_start, data_interval_end)

# COMMAND ----------

#read list of locations
raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location=raw_locations,
    csv_has_headers=True,
    csv_delimiter="|~|",
    csv_escape_character="\"",
)

# COMMAND ----------

if incremental_load.lower() == 'true':
    raw_df = raw_df.filter("header__change_oper != 'B'")

# COMMAND ----------

clean_columns_df = transform_utils.clean_column_names(dbutils, raw_df)

# COMMAND ----------

transformed_df = (
    transform_utils.cast_all_columns_to_string(dbutils=dbutils, df=clean_columns_df)
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

print(vars(results))

# COMMAND ----------

#Add last read partition to results which will be passed to adf
vars(results)["data_interval_start"] = data_interval_start
vars(results)["data_interval_end"] = data_interval_end

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
