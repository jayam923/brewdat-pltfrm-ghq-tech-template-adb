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
    #location="dbfs:/FileStore/dataquality/export.csv",
    #location="/FileStore/tables/testing_UAT_Business_Glossary__1_.xlsx",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
    #excel_sheet_name = "businessGlossary"
)

display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import *
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
#clean_df = data_quality_utils.null_check(field_name = "RegistrationNo" ,src_df = clean_df)
display(clean_df)

# COMMAND ----------

# DBTITLE 1,Reading Json files
from pyspark.sql.functions import col, count, lit, length, when, array_union, array
import pyspark.sql.functions as f
import pandas as pd
json_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.JSON,
    location="dbfs:/FileStore/dataquality/config_rule_final-1.json")
display(json_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

jdbcServername = "szmas-asrsqlr.sabmaa.africa.gcn.local/sql2"
jdbcPort = "50909"
jdbcDatabase = "SysproCompanyN_Reporting"
username = "AFRBrewDatTalendS"
password = "9%kLZV%B#4JE"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcServername,jdbcPort, jdbcDatabase, username, password)
pushdown_query = "dbo.AdmCtlOps"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query)
display(df)

# COMMAND ----------

jdbcServername = "edhazurencertdmgbdev.database.windows.net"
jdbcDatabase = "edhazurencertdmgbdev"
username = "sqldwdevuser_brewright1_ro"
password = "M1mnXzsWV757bncXqwerts2bczg"
jdbcUrl = "jdbc:sqlserver://{0};database={1};user={2};password={3}".format(jdbcServername, jdbcDatabase, username, password)
pushdown_query = "abi_compliance_africa_az.grndetails_az"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query)
display(df)


# COMMAND ----------

clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
#clean_df = data_quality_utils.data_type_check(dbutils =dbutils,field_name = "RegistrationNo" ,data_type = "decimal", df = clean_df) 
#clean_df = data_quality_utils.null_check(dbutils =dbutils,field_name = "Literacy" ,df = clean_df)
#clean_df = data_quality_utils.max_length(dbutils =dbutils,field_name = "Literacy" ,maximum_length = 5, df = clean_df)
#clean_df = data_quality_utils.min_length(dbutils =dbutils,field_name = "Literacy" ,minimum_length = 5, df = clean_df)
#clean_df = data_quality_utils.range_value(dbutils =dbutils,field_name = "Literacy" , minimum_value = 80,maximum_value = 90, df = clean_df)
#clean_df = data_quality_utils.valid_values(dbutils =dbutils,field_name = "Sex" ,valid_values=['Male','Others'],df = clean_df) 
clean_df = data_quality_utils.invalid_values(dbutils =dbutils,field_name = "Sex" ,invalid_values=['Female','null'],df = clean_df)   
#clean_df = data_quality_utils.valid_regular_expression(dbutils =dbutils,field_name = "Lname" ,regex="^[a-s]",df = clean_df)
#clean_df = data_quality_utils.duplicate_check(dbutils =dbutils,col_list = ["Name","Sex","Lname"],df = clean_df)
#tes_df = data_quality_utils.column_check(dbutils =dbutils,col_list=['Lname','Salary',"test"], df = clean_df)
#display(clean_df['Name','Lname','Sex','__bad_record','__data_quality_issues'])
display(clean_df['Sex','__bad_record','__data_quality_issues'])

# COMMAND ----------

#********************************to check if the employeNo column has data type as string or not.
#clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
#clean_df = data_quality_utils.data_type_check(field_name = "EmployeeNo" ,data_type = "Integer", src_df = clean_df)

#*******************************to check if the salary column has Null value
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.null_check(field_name = "Salary" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "Sex" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "RegistrationNo" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "Avgdays" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "EmployeeNo" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "Name" ,src_df = clean_df)
# clean_df = data_quality_utils.null_check(field_name = "Lname" ,src_df = clean_df)

#********************************to check Assert numeric column value to be between/less/greater
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.range_value(field_name = "Salary" , minimum_value = 46000,maximum_value = 60000, src_df = clean_df)
# clean_df = data_quality_utils.range_value(field_name = "Population" , minimum_value = 6731790,maximum_value = 7000000, src_df = clean_df)
# clean_df = data_quality_utils.range_value(field_name = "Sexratio" , minimum_value = 800,maximum_value = 900, src_df = clean_df)??

#*********************************to check Assert column value to be in set of valid values
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.valid_values(field_name = "Lname" ,valid_values=['sun', 'mon'],src_df = clean_df)
# clean_df = data_quality_utils.valid_values(field_name = "City" ,valid_values="India",src_df = clean_df)
# clean_df = data_quality_utils.valid_values(field_name = "State" ,valid_values=['Bihar','Uttar Pradesh'],src_df = clean_df)

#************************************ to check Assert column value not to be in set of invalid values 
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.invalid_values(field_name = "Name" ,invalid_values=['Jan','Feb','Mar'],src_df = clean_df)
# clean_df = data_quality_utils.invalid_values(field_name = "Sex" ,invalid_values=['Male'],src_df = clean_df)
# clean_df = data_quality_utils.invalid_values(field_name = "City" ,invalid_values=['Bangalore','Chennai'],src_df = clean_df)

# #************************************ to check Assert string length to be between/less/greater
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.max_length(field_name = "State" ,maximum_length = 5, src_df = clean_df)
# clean_df = data_quality_utils.max_length(field_name = "City" ,maximum_length = 9, src_df = clean_df)

#*************************************** to check Assert string length to be between/less
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.min_length(field_name = "City" ,minimum_length = 5, src_df = clean_df)
# clean_df = data_quality_utils.min_length(field_name = "State" ,minimum_length = 5, src_df = clean_df)

#*************************************** to check Assert column value not to match regex
# clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
# clean_df = data_quality_utils.valid_regular_expression(field_name = "Lname" ,valid_regular_expression="^[s-t]",src_df = clean_df)

#**************************************** to check  Apart from this we have implemented duplicate_check function to filter the bad records as well
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
clean_df = data_quality_utils.duplicate_check(col_list = ["Name","Lname"],src_df = clean_df)

display(clean_df)

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location="dbfs:/FileStore/dataquality/DQData_1.csv",
    #location="/FileStore/tables/testing_UAT_Business_Glossary__1_.xlsx",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
    #excel_sheet_name = "businessGlossary"
)

display(raw_df)

# COMMAND ----------

# DBTITLE 1,to test individual function
clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
#clean_df = data_quality_utils.data_type_check(field_name = "Salary" ,data_type = "Integer", src_df = clean_df) 
#clean_df = data_quality_utils.null_check(field_name = "RegistrationNo" ,src_df = clean_df)
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
display(clean_df['Lname','Salary','State','__bad_record','__data_quality_issues'])


# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking with edited file

# COMMAND ----------

raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location="dbfs:/FileStore/dataquality/Book1.csv",
    #location="/FileStore/tables/testing_UAT_Business_Glossary__1_.xlsx",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
    #excel_sheet_name = "businessGlossary"
)

display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import *
edited_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
display(edited_df)

# COMMAND ----------

#edited_df = data_quality_utils.data_type_check(field_name = "City" ,data_type = "String", src_df = edited_df)
#checking with regular expression
#edited_df = data_quality_utils.valid_regular_expression(field_name = "Salary" ,valid_regular_expression="^[s-t]",src_df = edited_df)
#edited_df = data_quality_utils.duplicate_check(col_list = ["Name","Lname"],src_df = edited_df)
#display(edited_df["EmployeeNo","__bad_record","__data_quality_issues"])


#clean_df = raw_df.withColumn('__bad_record',lit('False')).withColumn('__data_quality_issues',array())
#edited_df = data_quality_utils.data_type_check(dbutils =dbutils,field_name = "Avgdays" ,data_type = "String", df = edited_df) 
edited_df = data_quality_utils.null_check(dbutils =dbutils,field_name = "Name" ,df = edited_df)
#edited_df = data_quality_utils.max_length(dbutils =dbutils,field_name = "Sex" ,maximum_length = 6, src_df = edited_df)
#edited_df = data_quality_utils.min_length(dbutils =dbutils,field_name = "Literacy" ,minimum_length = 4, src_df = edited_df)
#edited_df = data_quality_utils.range_value(dbutils =dbutils,field_name = "Salary" , minimum_value = 33000,maximum_value = 60000, src_df = edited_df)
#edited_df = data_quality_utils.valid_values(dbutils =dbutils,field_name = "Literacy" ,valid_values=['89.73','82.5'],src_df = edited_df) 
#edited_df = data_quality_utils.invalid_values(dbutils =dbutils,field_name = "Literacy" ,invalid_values=['abc','89.73'],src_df = edited_df)   
#edited_df = data_quality_utils.valid_regular_expression(dbutils =dbutils,field_name = "Lname" ,valid_regular_expression="^[a-s]",src_df = edited_df)
#edited_df = data_quality_utils.duplicate_check(dbutils =dbutils,col_list = ["Name","EmployeeNo","Name","Lname","Sex","Salary","City","State"],src_df = edited_df)
#tes_df = data_quality_utils.column_check(dbutils =dbutils,col_list=['Lname','Salary',"test"], src_df = clean_df)
#display(edited_df['RegistrationNo','Name','Lname','Salary','City','State','Population','__bad_record','__data_quality_issues'])
display(edited_df['Name','__bad_record','__data_quality_issues'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###FOR WIDE CHECK

# COMMAND ----------

result_list = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location="dbfs:/FileStore/dataquality/Book2.csv",
    #location="dbfs:/FileStore/dataquality/export.csv",
    #location="/FileStore/tables/testing_UAT_Business_Glossary__1_.xlsx",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
    #excel_sheet_name = "businessGlossary"
)

display(result_list)

# COMMAND ----------


# Create object for Great Expectation
validator, result_list = data_quality_wide_checks.configure_great_expectation(result_list)


#run functions which can be run directly using greate expectations
test= data_quality_wide_checks.dq_validate_column_unique_values(dbutils =dbutils ,validator ,col_name = "EmployeeNo", mostly =0.8, resultlist = result_list )
#test= data_quality_wide_checks.dq_validate_compond_column_unique_values(dbutils ,validator ,col_names = ["SalesOrderID"], mostly =0.8, resultlist = result_list)
#test= data_quality_wide_checks.dq_validate_row_count(dbutils ,validator ,row_count = 40, mostly =0.9, resultlist = result_list)
#test= data_quality_wide_checks.dq_validate_column_nulls_values(dbutils ,validator ,col_name = "SalesOrderID", mostly =0.8, resultlist = result_list)
#test= data_quality_wide_checks.dq_validate_range_for_numeric_column_sum_values(dbutils, validator, col_name = "testing", min_value= 500 , max_value = 1000, resultlist = result_list)  # to run this function we need integer column

# run function which depends on delta table  
#count variation 
#current_df, history_df = data_quality_wide_checks.get_delta_tables_history_dataframe(spark = spark, dbutils =dbutils,  target_location = target_location, results = _testing)
#test = data_quality_wide_checks.dq_validate_count_variation_from_previous_version_values(dbutils ,validator ,history_df, current_df, resultlist = result_list)

# #Null  variation 
#current_df, history_df = data_quality_wide_checks.get_delta_tables_history_dataframe(spark = spark,dbutils =dbutils,  target_location = target_location, results = _testing)
#result = data_quality_wide_checks.dq_validate_null_percentage_variation_from_previous_version_values(dbutils ,history_df, current_df, "SalesOrderID", resultlist = result_list)


#result of all validation in dataframe
#final_result_df = data_quality_wide_checks.get_wider_dq_results(spark=spark, dbutils=dbutils, values= result_list)
#display(final_result_df)
display(test)

# COMMAND ----------

# COMMAND ----------


