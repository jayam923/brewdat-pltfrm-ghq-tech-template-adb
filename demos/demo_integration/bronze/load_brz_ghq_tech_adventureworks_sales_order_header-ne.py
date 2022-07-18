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
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils, dataquality_utils, QC_utils, qualityCheck_utils

# Print a module's help
help(QC_utils)

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
    location="dbfs:/FileStore/dataquality/DQData.csv",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)

display(raw_df)

# COMMAND ----------

clean_df = transform_utils.clean_column_names(dbutils=dbutils, df=raw_df)

display(clean_df)

# COMMAND ----------

from pyspark.sql.functions import col
df = read_utils.read_raw_dataframe(
    spark=spark,
    dbutils=dbutils,
    file_format=read_utils.RawFileFormat.JSON,
    location="dbfs:/FileStore/dataquality/config_rule.json",
)
temp = transform_utils.flatten_dataframe(
    df= df, 
    dbutils=dbutils)
display(temp)



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

# COMMAND ----------

try:
    err, temp =qualityCheck_utils.run_validation(file_name = "test",src_df = clean_df ,file_path_rules = logic_df ,reject_Path =None)
    display(err)
    display(temp)
except Exception:
        common_utils.exit_with_last_exception(dbutils)

# COMMAND ----------



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

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, target_location)
fullHistoryDF = deltaTable.history()
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

df1 = spark.read.format("delta").option("versionAsOf", 39).load(target_location)
df2 = spark.read.format("delta").option("versionAsOf", 35).load(target_location)
df3= df1.subtract(df2)
print(df3.count())



# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
