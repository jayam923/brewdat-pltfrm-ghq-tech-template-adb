# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.2.0", "1 - brewdat_library_version")
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

dbutils.widgets.text("target_hive_table", "monthly_sales_order", "6 - target_hive_table")
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
from brewdat.data_engineering.transform  import deduplicate_records, create_or_replace_audit_columns
from brewdat.data_engineering.lakehouse  import generate_gold_table_location
from brewdat.data_engineering.write      import write_delta_table, SchemaEvolutionMode, LoadType  
from brewdat.data_engineering.common     import exit_with_object


help(read_raw_dataframe)
help(RawFileFormat)
help(create_or_replace_audit_columns)
help(generate_gold_table_location)
help(write_delta_table)
help(SchemaEvolutionMode)
help(LoadType)
help(exit_with_object)

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

df = spark.sql("""
        SELECT
            DATE_FORMAT(order_header.order_date, 'yyyy-MM') AS order_year_month,
            order_header.online_order_flag,
            customer.gender AS customer_gender,
            ship_to_address.country_region AS ship_to_country_region,
            ship_to_address.state_province AS ship_to_state_province,
            ship_to_address.city AS ship_to_city,
            COUNT(order_header.sales_order_id) AS total_orders,
            SUM(order_header.sub_total) AS sum_sub_total
        FROM
            slv_ghq_tech_adventureworks.sales_order_header AS order_header
            INNER JOIN slv_ghq_tech_adventureworks.customer
                ON customer.customer_id = order_header.customer_id
            INNER JOIN slv_ghq_tech_adventureworks.address AS ship_to_address
                ON ship_to_address.address_id = order_header.ship_to_address_id
        WHERE
            order_header.status_code NOT IN (
                1, -- In Process
                4, -- Rejected
                6 -- Canceled
            )
        GROUP BY
            order_year_month,
            online_order_flag,
            customer_gender,
            ship_to_country_region,
            ship_to_state_province,
            ship_to_city
        WITH ROLLUP
    """.format(
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    ))

#display(df)

# COMMAND ----------

audit_df = create_or_replace_audit_columns(df)

#display(audit_df)

# COMMAND ----------

location = generate_gold_table_location(
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    project=project,
    database_name=target_hive_database,
    table_name=target_hive_table,
)

results = write_delta_table(spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=LoadType.OVERWRITE_TABLE,
    schema_evolution_mode=SchemaEvolutionMode.OVERWRITE_SCHEMA,
)

print(results)

# COMMAND ----------

exit_with_object(dbutils,results)
