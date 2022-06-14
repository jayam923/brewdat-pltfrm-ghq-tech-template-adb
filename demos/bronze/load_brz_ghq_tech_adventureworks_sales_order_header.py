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

import os
import sys

# Import the BrewDat Library
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering.read       import read_raw_dataframe
from brewdat.data_engineering.read       import RawFileFormat
from brewdat.data_engineering.transform  import clean_column_names, create_or_replace_audit_columns
from brewdat.data_engineering.lakehouse  import generate_bronze_table_location
from brewdat.data_engineering.write      import write_delta_table, SchemaEvolutionMode, LoadType  
from brewdat.data_engineering.common     import exit_with_object, ReturnObject 


help(read_raw_dataframe)
help(RawFileFormat)
help(clean_column_names)
help(create_or_replace_audit_columns)
help(generate_bronze_table_location)
help(write_delta_table)
help(SchemaEvolutionMode)
help(LoadType)
help(exit_with_object)


# COMMAND ----------

# Gather standard Lakehouse environment variables
environment = os.getenv("ENVIRONMENT")
lakehouse_raw_root = os.getenv("LAKEHOUSE_RAW_ROOT")
lakehouse_bronze_root = os.getenv("LAKEHOUSE_BRONZE_ROOT")

# Ensure that all standard Lakehouse environment variables are set
if None in [environment, lakehouse_raw_root, lakehouse_bronze_root]:
    raise Exception("This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster.")

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
if environment == "dev":
    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", "1d3aebfe-929c-4cc1-a988-31c040d2b798")
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="brewdatpltfrmghqtechakvd", key="brewdat-spn-pltfrm-ghq-tech-template-rw-d"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/token")
elif environment == "qa":
    # TODO: update
    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", "12345678-1234-1234-1234-123456789999")
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="brewdatpltfrmghqtechakvq", key="brewdat-spn-pltfrm-ghq-tech-template-rw-q"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/token")
elif environment == "prod":
    # TODO: update
    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", "12345678-1234-1234-1234-123456789999")
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="brewdatpltfrmghqtechakvp", key="brewdat-spn-pltfrm-ghq-tech-template-rw-p"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/token")

# COMMAND ----------

raw_df = read_raw_dataframe(spark,
    file_format=RawFileFormat.CSV,
    location=f"{lakehouse_raw_root}/data/ghq/tech/adventureworks/adventureworkslt/saleslt/salesorderheader/",
    csv_has_headers=True,
    csv_delimiter=",",
    csv_escape_character="\"",
)
#display(raw_df)


# COMMAND ----------

clean_df = clean_column_names(raw_df)

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

audit_df = create_or_replace_audit_columns(transformed_df)

#display(audit_df)

# COMMAND ----------

target_location = generate_bronze_table_location(
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)

results = write_delta_table(spark,
    df=audit_df,
    location=target_location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=LoadType.APPEND_ALL,
    partition_columns=["__ref_dt"],
    schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(results)

# COMMAND ----------

exit_with_object(dbutils,results)
