# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("data_product", "demo_consumption", "2 - data_product")
data_product = dbutils.widgets.get("data_product")
print(f"data_product: {data_product}")

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

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

key_columns = ["SalesOrderID"]

df = spark.sql("""
        SELECT 
            SalesOrderID,
            StatusDescription,
            OnlineOrderFlag,
            SalesOrderNumber,
            order_header.CustomerID,
            PurchaseOrderNumber,
            order_header.__update_gmt_ts
        FROM 
            slv_ghq_tech_adventureworks.sales_order_header AS order_header 
            LEFT JOIN slv_ghq_tech_adventureworks.customer AS customer      
                ON order_header.CustomerID = customer.CustomerID
        WHERE
            order_header.__update_gmt_ts BETWEEN '{data_interval_start}' AND '{data_interval_end}'
    """.format(
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    ))

#display(df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=df,
    key_columns=key_columns,
    watermark_column="__update_gmt_ts",
)

#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_gold_table_location(
    dbutils=dbutils,
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    data_product=data_product,
    database_name=target_hive_database,
    table_name=target_hive_table,
)
print(f"location: {location}")

# COMMAND ----------

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    key_columns=key_columns,
    location=location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
