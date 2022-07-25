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
    location="dbfs:/FileStore/dataquality/DQData_1.csv",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, lit, length, when, array_union, array
import pyspark.sql.functions as f
import pandas as pd
json_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.JSON,
    location="dbfs:/FileStore/dataquality/viv_values_json.json")
display(json_df)

# COMMAND ----------

# DBTITLE 1,to test individual 
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__failed_dg_check',array())
#clean_df = data_quality_utils.data_type_check(spark=spark, dbutils=dbutils,file_name ="test", field_name = "Salary" ,data_type = "Integer" ,src_df = clean_df) 
#clean_df = data_quality_utils.null_check(spark=spark, dbutils=dbutils,file_name ="test",field_name = "RegistrationNo" ,src_df = clean_df)
#clean_df = data_quality_utils.max_length(spark=spark, dbutils=dbutils,file_name ="test",field_name = "Sex" ,maximum_length = 6, src_df = clean_df)
#clean_df = data_quality_utils.min_length(spark=spark, dbutils=dbutils,file_name ="test",field_name = "City" ,minimum_length = 5, src_df = clean_df)
#clean_df = data_quality_utils.range_value(spark=spark, dbutils=dbutils,file_name ="test",field_name = "Salary" , minimum_value = 10000,maximum_value = 60000, src_df = clean_df)
#clean_df = data_quality_utils.valid_values(spark=spark, dbutils=dbutils,file_name = "test",field_name = "Lname" ,valid_values=['sun', 'mon'],src_df = clean_df) 
#clean_df = data_quality_utils.invalid_values(spark=spark, dbutils=dbutils,file_name = "test",field_name = "Lname" ,invalid_values=['tue', 'wed', 'thu'],src_df = clean_df)   
#clean_df = data_quality_utils.valid_regular_expression(spark=spark, dbutils=dbutils, file_name = "test", field_name = "Lname" ,valid_regular_expression="^[s-t]",src_df = clean_df)
#clean_df = data_quality_utils.duplicate_check(spark=spark, dbutils=dbutils, col_list = ["Name","EmployeeNo","Lname"],file_name = "test",src_df = clean_df)
tes_df = data_quality_utils.column_check(spark=spark, dbutils=dbutils, col_list=['Lname','Salary',"test"], src_df = clean_df,file_name = "test" )
display(clean_df)
print(tes_df)

# COMMAND ----------

# DBTITLE 1,To run DQ check using Json file
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__failed_dg_check',array())
clean_df = data_quality_utils.run_validation(spark=spark, dbutils=dbutils, file_name = "test",src_df = clean_df, file_path_rules=json_df)
display(clean_df)


# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    dbutils=dbutils,
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)



# COMMAND ----------

from pyspark.sql.functions import when
# display(clean_df)
# display(temp)
result_data = clean_df.union(temp)
display(result_data)
# df3 = clean_df.withColumn("bad_record", when(col(temp.bad_record == "True","True") \
#       .otherwise(clean_df.bad_record))
# df3.show(30)


# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, target_location)
fullHistoryDF = deltaTable.history(1)
display(fullHistoryDF)

# COMMAND ----------

#temp = fullHistoryDF.filter(fullHistoryDF.operation == "WRITE").select("operationParameters")
temp = fullHistoryDF.filter(fullHistoryDF.operation == "WRITE").select("operationParameters")
display(temp)
print(type(temp))


# COMMAND ----------

import pyspark.sql.functions as F
temp = fullHistoryDF.filter(fullHistoryDF.operation == "VACUUM END").select(F.max("version")).first()[0]
print(temp)

# COMMAND ----------

latest = spark.read.format("delta").option("versionAsOf", 39).load(target_location)
print(latest.count())
history = spark.read.format("delta").option("versionAsOf", 35).load(target_location)
print(history.count())
# df3= latest.subtract(history)
# print(df3.count())
# return df3



# COMMAND ----------

contextval = QualityCheck_wide_utils.configure_data_context()
batch_request_t = QualityCheck_wide_utils.Create_batch_request(dbutils= dbutils, df=latest, context = contextval )
validator_t = QualityCheck_wide_utils.Create_expectation_suite(dbutils= dbutils, df=latest, context = contextval, batch_request =batch_request_t)
batch_request_2 = QualityCheck_wide_utils.Create_batch_request(dbutils= dbutils, df=history, context = contextval )
validator_2 = QualityCheck_wide_utils.Create_expectation_suite(dbutils= dbutils, df=history, context = contextval, batch_request =batch_request_2)

# COMMAND ----------

his, lal= QualityCheck_wide_utils.dq_validate_count_variation_percentage_from_previous_version_values(
  dbutils= dbutils,
  current_validator = validator_2, 
  null_percentage = .8 ,
  col_name = "RevisionNumber",
  history_validator =validator_t)

# COMMAND ----------

result = QualityCheck_wide_utils.dq_validate_column_nulls_values (dbutils= dbutils, null_percentage = .8 ,col_name = "RevisionNumber",validator =validator_t)
#print(result)
print(type(result['result']['element_count']))
# print(result['result']['unexpected_percent'])
# resultw = QualityCheck_wide_utils.dq_validate_column_nulls_values (dbutils= dbutils, null_percentage = .8 ,col_name = "RevisionNumber",validator =validator_2)
# print(resultw['result']['element_count'])
# print(result['result']['unexpected_percent'])
# #print(result)


# COMMAND ----------

result = QualityCheck_wide_utils.dq_validate_row_count_to_be_in_between_range(dbutils, validator_t,10, 20)
print(result)


# COMMAND ----------

logic_df= temp.select(col('Metadata__tag__DataType').alias("DataType"),
    col('Metadata__tag__FieldName').alias("FieldName"),
    col('Metadata__tag__FileName').alias("FileName"),
    col('Metadata__tag__IsNull').alias("IsNull"),
    col('Metadata__tag__Maximum_Length').alias("Maximum_Length"),
    col('Metadata__tag__Minimum_Length').alias("Minimum_Length"),
    col('Metadata__tag__Maximum_value').alias("Maximum_value"),
    col('Metadata__tag__Minimum_value').alias("Minimum_value"),
    col('Metadata__tag__PK').alias("PK"),
    col('Metadata__tag__Valid_Regular_Expression').alias("Valid_Regular_Expression"),
    col('Metadata__tag__Valid_Values').alias("Valid_Values"),
    col('Metadata__tag__Invalid_Values').alias("Invalid_Values"))
