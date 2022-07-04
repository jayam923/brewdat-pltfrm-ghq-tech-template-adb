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

import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

column_name_separator = ":"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=[adls_raw_bronze_storage_account_name],
        key_vault_name=key_vault_name,
        spn_client_id=spn_client_id,
        spn_secret_name=spn_secret_name,
    )

# COMMAND ----------

dit = {}
def flattening_check(unflattened_df):
    print("Inside flattening function")
    dit['main_df'] = unflattened_df
    for col_name, col_type in unflattened_df.dtypes:

        if col_type[:6] == 'struct':
            dit['main_df'] = flatten_df(dit['main_df'])

        if col_type[:5] == 'array':
            dit['main_df'] = explode_array(dit['main_df'], col_name)

    print(dit['main_df'].printSchema())

    struct_types = [c[0] for c in dit['main_df'].dtypes if c[1][:6] == 'struct']
    array_types = [c[0] for c in dit['main_df'].dtypes if c[1][:5] == 'array']
    if len(struct_types) > 0 or len(array_types) > 0:
        ##dit['main_df'].persist(StorageLevel.MEMORY_AND_DISK)
        flattening_check(dit['main_df'])

    return dit['main_df']

def flatten_df(nested_df):
    print("Visited flatten df")
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols + [F.col(nc+'.'+c).alias(nc+'__'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
    new_column_name_list= list(map(lambda x: x.replace(column_name_separator, "_"), flat_df.columns))
    flat_df = flat_df.toDF(*new_column_name_list)
    return flat_df


def explode_array(nested_df, column):
    print("Visited array explode")
    explode_col_name = nested_df.select(column)
    exploded_df = nested_df.withColumn(column, explode_outer(explode_col_name[0]))
    return exploded_df

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.XML,
    location=f"{lakehouse_raw_root}/data/ghq/tech/workday_soap_api/worker_time_off/__ref_dt=20220704/Worker_Timeoff",
    xml_row_tag="wd:Report_Entry",
)

display(raw_df)

# COMMAND ----------

# raw_df = read_utils.read_raw_dataframe(
#     spark=spark,
#     dbutils=dbutils,
#     file_format=read_utils.RawFileFormat.CSV,
#     location=f"{lakehouse_raw_root}/data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/",
#     csv_has_headers=True,
#     csv_delimiter=",",
#     csv_escape_character="\"",
# )

# #display(raw_df)

# COMMAND ----------

clean_df = transform_utils.clean_column_names(dbutils=dbutils, df=raw_df)

display(clean_df)

# COMMAND ----------

flatten_df = flattening_check(clean_df)
flatten_df = flatten_df.withColumn("__ref_dt",F.date_format(F.current_date(),'yyyyMMdd'))
display(flatten_df)

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = (
    flatten_df
    .filter(F.col("__ref_dt").between(
        F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
        F.date_format(F.lit(data_interval_end), "yyyyMMdd"),
    ))
    .withColumn("__src_file", F.input_file_name())
)

display(transformed_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=transformed_df)

display(audit_df)

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