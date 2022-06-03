# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.1.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("project", "demo_consumption", "2 - project")
project = dbutils.widgets.get("project")
print(f"project: {project}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "gld_ghq_tech_demo_consumption", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "customer_orders", "6 - target_hive_table")
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
from brewdat.data_engineering.utils import BrewDatLibrary

# Initialize the BrewDat Library
brewdat_library = BrewDatLibrary(spark=spark, dbutils=dbutils)
help(brewdat_library)

# COMMAND ----------

# Gather standard Lakehouse environment variables
environment = os.getenv("ENVIRONMENT")
lakehouse_silver_root = os.getenv("LAKEHOUSE_SILVER_ROOT")
lakehouse_gold_root = os.getenv("LAKEHOUSE_GOLD_ROOT")

# Ensure that all standard Lakehouse environment variables are set
if None in [environment, lakehouse_silver_root, lakehouse_gold_root]:
    raise Exception("This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster.")

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "1d3aebfe-929c-4cc1-a988-31c040d2b798")
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="brewdatpltfrmghqtechakvd", key="brewdat-spn-pltfrm-ghq-tech-template-rw-d"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/token")

# COMMAND ----------

key_columns = ["sales_order_id"]

df = spark.sql("""
        SELECT 
            sales_order_id, 
            status_code, 
            online_order_flag, 
            sales_order_number, 
            order_header.customer_id, 
            purchase_order_number,
            order_header.__update_gmt_ts
        FROM 
            slv_ghq_tech_adventureworks.sales_order_header AS order_header 
            LEFT JOIN slv_ghq_tech_adventureworks.customer AS customer      
                ON order_header.customer_id = customer.customer_id
        WHERE
            order_header.__update_gmt_ts BETWEEN '{data_interval_start}' AND '{data_interval_end}'
    """.format(
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    ))

#display(df)

# COMMAND ----------

dedup_df = brewdat_library.deduplicate_records(
    df=df,
    key_columns=key_columns,
    watermark_column="__update_gmt_ts",
)

#display(dedup_df)

# COMMAND ----------

audit_df = brewdat_library.create_or_replace_audit_columns(dedup_df)

#display(audit_df)

# COMMAND ----------

location = brewdat_library.generate_gold_table_location(
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    project=project,
    database_name=target_hive_database,
    table_name=target_hive_table,
)

results = brewdat_library.write_delta_table(
    df=audit_df,
    key_columns=key_columns,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=brewdat_library.LoadType.UPSERT,
    schema_evolution_mode=brewdat_library.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(results)

# COMMAND ----------

brewdat_library.exit_with_object(results)
