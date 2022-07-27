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
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils, data_quality_utils, data_quality_wide_checks

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
    location="dbfs:/FileStore/dataquality/DQData_1.csv",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)

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

# DBTITLE 1,to test individual function
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
clean_df = data_quality_utils.data_type_check(field_name = "Salary" ,data_type = "Integer", src_df = clean_df) 
clean_df = data_quality_utils.null_check(field_name = "RegistrationNo" ,src_df = clean_df)
#clean_df = data_quality_utils.max_length(field_name = "Sex" ,maximum_length = 6, src_df = clean_df)
#clean_df = data_quality_utils.min_length(field_name = "City" ,minimum_length = 5, src_df = clean_df)
#clean_df = data_quality_utils.range_value(field_name = "Salary" , minimum_value = 10000,maximum_value = 60000, src_df = clean_df)
clean_df = data_quality_utils.valid_values(field_name = "Lname" ,valid_values=['sun', 'mon'],src_df = clean_df) 
#clean_df = data_quality_utils.invalid_values(field_name = "Lname" ,invalid_values=['tue', 'wed', 'thu'],src_df = clean_df)   
#clean_df = data_quality_utils.valid_regular_expression(field_name = "Lname" ,valid_regular_expression="^[s-t]",src_df = clean_df)
#clean_df = data_quality_utils.duplicate_check(col_list = ["Name","EmployeeNo","Lname"],src_df = clean_df)
#tes_df = data_quality_utils.column_check(col_list=['Lname','Salary',"test"], src_df = clean_df)
display(clean_df)
#print(tes_df)

# COMMAND ----------

# DBTITLE 1,To run DQ check using Json file
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
clean_df = data_quality_utils.run_validation(spark=spark, dbutils=dbutils, src_df = clean_df, json_df=json_df)
display(clean_df)


# COMMAND ----------


