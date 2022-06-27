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

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# MAGIC %run ../../demo_context

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

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_gold_table_location(
    dbutils=dbutils,
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    project=project,
    database_name=target_hive_database,
    table_name=target_hive_table,
)

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.OVERWRITE_TABLE,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.OVERWRITE_SCHEMA,
)

print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
