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

print(lakehouse_raw_root)
raw_df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.CSV,
    location=f"{lakehouse_raw_root}/data/ghq/tech/old_manish_files/csv/",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)

# COMMAND ----------

clean_df = transform_utils.clean_column_names(dbutils=dbutils, df=raw_df)

display(clean_df)

# COMMAND ----------

# DBTITLE 1,Entry point for data quality to create, configure and access the expectations and checkpoints
contextval = data_quality_utils.configure_data_context()
batch_request_t = data_quality_utils.Create_batch_request(dbutils= dbutils, df=clean_df, context = contextval )
validator_t = data_quality_utils.Create_expectation_suite(dbutils= dbutils, df=clean_df, context = contextval, batch_request =batch_request_t)

# COMMAND ----------

data_quality_utils.dq_validate_column_exist (dbutils= dbutils, col_name = "SalesOrderNumber",validator =validator_t)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,calling few data quality validation rule on few columns just for the demo
data_quality_utils.dq_validate_column_exist (dbutils= dbutils, col_name = "SalesOrderNumber",validator =validator_t)
data_quality_utils.dq_validate_column_type(dbutils= dbutils, col_name = "SalesOrderNumber", col_type = "IntegerType",validator =validator_t )
data_quality_utils.dq_validate_column_nulls_values(dbutils= dbutils, col_name = "SalesOrderID", validator =validator_t )
#data_quality_utils.dq_validate_column_min_values_between (dbutils= dbutils, col_name="Freight", min_value=600, max_value=650, validator =validator_t )
#data_quality_utils.dq_validate_column_max_values_between (dbutils= dbutils, col_name="ShipToAddressID", min_value=1000, max_value=1050, validator =validator_t )
data_quality_utils.dq_validate_column_values_to_be_in_set(dbutils= dbutils,col_name= "ShipMethod",col_values = ['CARGO TRANSPORT 5'] ,validator =validator_t )
data_quality_utils.dq_validate_column_values_to_not_match_regex(dbutils= dbutils,col_name= "ShipMethod",reg_exp = "['CARGO TRANSPORT 5']" ,validator =validator_t )
data_quality_utils.dq_validate_column_unique_values(dbutils= dbutils, col_name = "SalesOrderID", validator =validator_t )


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,After methods which are required to save and collect all the results from the validation
data_quality_utils.save_expectation_suite_in_validator(dbutils= dbutils,validator =validator_t)
results =data_quality_utils.get_dq_checkpoint_result(dbutils= dbutils,validator =validator_t,context = contextval, batch_request =batch_request_t )

# COMMAND ----------

# DBTITLE 1,fetching few required values from the results of each dq check 
for x in results['run_results']:
    validation_rule  = results['run_results'][x]['validation_result']['results']
    for result in range(len(validation_rule)):
        print("")
        print("--------result for each of the validation-------------")
        print("Result_success --> " + str(validation_rule[result]['success']))
        print("exception_info--> raised_exception--->" + str(validation_rule[result]['exception_info']['raised_exception']))
        print("exception_info--> exception_message----->" + str(validation_rule[result]['exception_info']['exception_message']))
        print("expectation_config--> expectation_type---->" + str(validation_rule[result]['expectation_config']['expectation_type']))
        values = validation_rule[result]['result']
        for actual_val in values:
            print(actual_val, values[actual_val])

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
print(target_location)
print(vars(results))

# COMMAND ----------

dbults.fs.ls(target_location)

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, target_location)
fullHistoryDF = deltaTable.history()
display(fullHistoryDF)

# COMMAND ----------

df = spark.read.format("delta").option("version", "40").load(target_location)
display(df)

#display(Sampledata)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
