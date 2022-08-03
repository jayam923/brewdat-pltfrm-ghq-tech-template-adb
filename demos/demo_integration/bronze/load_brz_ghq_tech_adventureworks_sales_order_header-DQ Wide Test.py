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

raw_df1 = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    #location=f"{lakehouse_raw_root}/data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/",
    location="dbfs:/FileStore/dataquality/DQ_Test_Files/Col_value_type_test.csv",
    csv_has_headers=False,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df1)

# COMMAND ----------

# DBTITLE 1,Direct function in GE 
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
#Add one column with integer values 
raw_df1=raw_df1.withColumn("testing", lit(20))

# Create object for Great Expectation
validator, result_list = data_quality_wide_checks.configure_great_expectation(raw_df1)

#run functions which can be run directly using greate expectations
test= data_quality_wide_checks.dq_validate_column_unique_values(dbutils ,validator ,col_name = "_c1", mostly =0.7, resultlist = result_list )
test= data_quality_wide_checks.dq_validate_compond_column_unique_values(dbutils ,validator ,col_list = ["_c1","_c3"], mostly =0.7, resultlist = result_list)
test= data_quality_wide_checks.dq_validate_row_count(dbutils ,validator, min_value =5, max_value = 30, resultlist = result_list)
test= data_quality_wide_checks.dq_validate_column_values_to_not_be_null(dbutils ,validator ,col_name = "_c3", mostly =0.8, resultlist = result_list)
test= data_quality_wide_checks.dq_validate_range_for_numeric_column_sum_values(dbutils, validator, col_name = "testing", min_value= 100 , max_value = 500, resultlist = result_list)  # to run this function we need integer column

# #result of all validation in dataframe/
final_result_df = data_quality_wide_checks.get_wider_dq_results(spark=spark, dbutils=dbutils, values= result_list)
display(final_result_df)


# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location=f"{lakehouse_raw_root}/data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/",
    #location="dbfs:/FileStore/dataquality/DQ_Test_Files/Col_value_type_test.csv",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = (
    raw_df
    .filter(F.col("__ref_dt").between(
        F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
        F.date_format(F.lit(data_interval_end), "yyyyMMdd"),
    ))
    .withColumn("__src_file", F.input_file_name())
)

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=transformed_df)
display(transformed_df)

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

print(results)
_testing = results

# COMMAND ----------

raw_df=raw_df.withColumn("testing", lit(20))
# # Create object for Great Expectation
validator, result_list = data_quality_wide_checks.configure_great_expectation(raw_df)

# #run functions which can be run directly using greate expectations
# test= data_quality_wide_checks.dq_validate_column_unique_values(dbutils ,validator ,col_name = "SalesOrderID", mostly =0.8, resultlist = result_list )
# print(test)
# test= data_quality_wide_checks.dq_validate_compond_column_unique_values(dbutils ,validator ,col_list = ["SalesOrderID"], mostly =0.8, resultlist = result_list)
# test= data_quality_wide_checks.dq_validate_row_count(dbutils ,validator, min_value =10, max_value = 30, resultlist = result_list)
# test= data_quality_wide_checks.dq_validate_column_values_to_not_be_null(dbutils ,validator ,col_name = "SalesOrderID", mostly =0.8, resultlist = result_list)
# test= data_quality_wide_checks.dq_validate_range_for_numeric_column_sum_values(dbutils, validator, col_name = "testing", min_value= 500 , max_value = 1000, resultlist = result_list)  # to run this function we need integer column

# #count variation  - function which depends on delta table  
test = data_quality_wide_checks.dq_validate_count_variation_from_previous_version_values(spark=spark, dbutils = dbutils , target_location = target_location, results = _testing, min_value = 100, max_value = 300, resultlist = result_list)

# # # Null variation - function which depends on delta table 
result = data_quality_wide_checks.dq_validate_null_percentage_variation_from_previous_version_values(spark=spark, dbutils = dbutils , target_location = target_location, results = _testing,  col_name = "SalesOrderID", dq_mostly = 0.1, resultlist = result_list)

# # #result of all validation in dataframe
final_result_df = data_quality_wide_checks.get_wider_dq_results(spark=spark, dbutils=dbutils, values= result_list)
display(final_result_df)

# COMMAND ----------

temp = 0.3463647
temp1 = 0.3125523
print(round(temp- temp1, 2))
print(round(temp,2)- round(temp1, 2))

# COMMAND ----------


