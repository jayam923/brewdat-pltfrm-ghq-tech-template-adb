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

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils, data_quality_utils

# Print a module's help
help(data_quality_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

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

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location="/FileStore/tables/DQData.csv",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)


# COMMAND ----------

data_quality_utils=data_quality_utils.DataQualityChecker(dbutils=dbutils,df=raw_df)

# COMMAND ----------

# DBTITLE 1,Test Individual Function
#clean_df = data_quality_utils.create_required_columns_for_dq_check(raw_df)

data_quality_utils.check_column_type_cast(column_name = "Avgdays" ,data_type = "double")
data_quality_utils.check_column_type_cast(column_name = "Salary" ,data_type = "Integer") 
data_quality_utils.check_column_is_not_null(column_name = "RegistrationNo" )
data_quality_utils.check_column_max_length(column_name = "City" ,maximum_length = 10)
#data_quality_utils.min_length( column_name = "City" , minimum_length 89.41= 5)
data_quality_utils.check_column_value_between( column_name = "Salary" , minimum_value = 10000,maximum_value = 30000)
data_quality_utils.check_column_value_is_in( column_name = "Lname" ,valid_values=['sun', 'mon']) 
#data_quality_utils.invalid_values( column_name = "Lname" ,invalid_values=['tue', 'wed', 'thu'])   
#data_quality_utils.valid_regular_expression( column_name = "_c1" ,regex="^[s-t]")
data_quality_utils.check_composite_column_value_is_unique(column_names = ['EmployeeNo'])
#data_quality_utils.column_check(column_names=['Lname','Salary',"test"])
build_df=data_quality_utils.build_df()
display(build_df)


# COMMAND ----------

# DBTITLE 1,Reading Json files
from pyspark.sql.functions import col, count, lit, length, when, array_union, array
import pyspark.sql.functions as f
import pandas as pd
json_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.JSON,
    location="dbfs:/FileStore/dataquality/config_rule_final.json")
display(json_df)

# COMMAND ----------

# DBTITLE 1,To Run DQ check using Json file
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
clean_df = data_quality_utils.run_validation(spark=spark, dbutils=dbutils, src_df = clean_df, json_df=json_df)
display(clean_df)


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

