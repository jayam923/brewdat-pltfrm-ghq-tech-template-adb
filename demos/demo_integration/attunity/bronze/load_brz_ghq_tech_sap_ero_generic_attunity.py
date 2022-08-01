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

dbutils.widgets.text("data_interval_end", "2022-06-22T00:00:00Z", "9 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

dbutils.widgets.text("incremental_load", "false", "10 - incremental_load")
incremental_load = dbutils.widgets.get("incremental_load")
print(f"incremental_load: {incremental_load}")

dbutils.widgets.text("watermark_column", "target_apply_ts", "10 - incremental_load")
watermark_column = dbutils.widgets.get("watermark_column")
print(f"watermark_column: {watermark_column}")

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
import pyspark.sql.functions as F

# COMMAND ----------

watermark_lower_bound, partition_start = data_interval_start.split('_')
watermark_upper_bound, partition_end = data_interval_end.split('_')

convert_watermark_format = lambda x : datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

raw_locations = []
base_table_path = f"{attunity_sap_ero_prelz_root}/attunity_test/test/{target_hive_table}/*.csv.gz"
base_df = read_utils.read_raw_dataframe(
                            spark=spark,
                            dbutils=dbutils,
                            file_format=read_utils.RawFileFormat.CSV,
                            location=[base_table_path],
                            csv_has_headers=True,
                            csv_delimiter="|~|",
                            csv_escape_character="\"",
                        )
#base_df.withColumn(watermark_column, col(watermark_column).cast(""))

if incremental_load.lower() == 'true':
    #build list of locations
    load_type = "APPEND_ALL"
    ct_table_path =f"{attunity_sap_ero_prelz_root}/attunity_test/test/{target_hive_table}__ct"
    last_partition_end_time = datetime.datetime.strptime(partition_start, "%Y-%m-%dT%H:%M:%SZ")
    partitions_to_load = read_utils.get_partitions_to_process(dbutils, ct_table_path, last_partition_end_time)
    if partitions_to_load:
        partition_start = partitions_to_load[0][1].strftime("%Y-%m-%dT%H:%M:%SZ")
        partition_end = partitions_to_load[-1][2].strftime("%Y-%m-%dT%H:%M:%SZ")
        raw_ct_locations = [f"{ct_table_path}/{partition[0]}/*.csv.gz" for partition in partitions_to_load]
        ct_df = read_utils.read_raw_dataframe(
                            spark=spark,
                            dbutils=dbutils,
                            file_format=read_utils.RawFileFormat.CSV,
                            location=raw_ct_locations,
                            csv_has_headers=True,
                            csv_delimiter="|~|",
                            csv_escape_character="\"",
                        )
        ct_df = ct_df.filter("header__change_oper != 'B'")
        union_df = base_df.unionByName(ct_df, allowMissingColumns=True)
        if not watermark_upper_bound:
            max_watermark = union_df.select(F.max(F.col(watermark_column))).collect()[0][0]
            watermark_upper_bound = convert_watermark_format(max_watermark)
            
        filtered_df = union_df.filter(F.col(watermark_column).between(
                            F.to_timestamp(F.lit(watermark_lower_bound)),
                            F.to_timestamp(F.lit(watermark_upper_bound)),
                        ))
        data_interval_start = f"{watermark_lower_bound}_{partition_start}" 
        data_interval_end = f"{watermark_upper_bound}_{partition_end}" 
    else:
        if not watermark_upper_bound:
            max_watermark = base_df.select(F.max(F.col(watermark_column))).collect()[0][0]
            watermark_upper_bound = convert_watermark_format(max_watermark)
        
        filtered_df = base_df.filter(F.col(watermark_column).between(
                            F.to_timestamp(F.lit(watermark_lower_bound)),
                            F.to_timestamp(F.lit(watermark_upper_bound)),
                        ))
        data_interval_start = f"{watermark_lower_bound}_{partition_start}" 
        data_interval_end = f"{watermark_upper_bound}_{partition_start}"
        
else:
    watermark_lower_bound = '0001-01-01T00:00:00Z'
    max_watermark = base_df.select(F.max(F.col(watermark_column))).collect()[0][0]
    watermark_upper_bound = convert_watermark_format(max_watermark)
    load_type = "OVERWRITE_TABLE"
    filtered_df = base_df.filter(F.col(watermark_column).between(
                            F.to_timestamp(F.lit(watermark_lower_bound)),
                            F.to_timestamp(F.lit(watermark_upper_bound)),
                        ))
    data_interval_start = f"{watermark_lower_bound}_{watermark_lower_bound}" 
    data_interval_end = f"{watermark_upper_bound}_{watermark_lower_bound}"
    

# COMMAND ----------

print(data_interval_start, data_interval_end)

# COMMAND ----------

clean_columns_df = transform_utils.clean_column_names(dbutils, filtered_df)

# COMMAND ----------

clean_columns_df.count()

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

#remove this line when table name is properly updated
target_hive_table = target_hive_table.split('.')[1]

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
    load_type=write_utils.LoadType(load_type),
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(vars(results))

# COMMAND ----------

#Add last read partition to results which will be passed to adf
vars(results)["data_interval_start"] = data_interval_start
vars(results)["data_interval_end"] = data_interval_end

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
