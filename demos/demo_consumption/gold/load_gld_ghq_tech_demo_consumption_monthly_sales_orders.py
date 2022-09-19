# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.5.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")

dbutils.widgets.text("target_zone", "ghq", "2 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "tech", "3 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_data_product", "demo_consumption", "4 - target_data_product")
target_data_product = dbutils.widgets.get("target_data_product")
print(f"{target_data_product = }")

dbutils.widgets.text("target_database", "gld_ghq_tech_demo_consumption", "5 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "monthly_sales_order", "6 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("data_interval_start", "2022-05-21T00:00:00Z", "7 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"{data_interval_start = }")

dbutils.widgets.text("data_interval_end", "2022-05-22T00:00:00Z", "8 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"{data_interval_end = }")

# COMMAND ----------

import sys

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    storage_account_names=[adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

try:
    df = spark.sql("""
        SELECT
            DATE_FORMAT(order_header.OrderDate, 'yyyy-MM') AS OrderYearMonth,
            order_header.OnlineOrderFlag,
            customer.Gender AS CustomerGender,
            ship_to_address.CountryRegion AS ShipToCountryRegion,
            ship_to_address.StateProvince AS ShipToStateProvince,
            ship_to_address.City AS ShipToCity,
            COUNT(order_header.SalesOrderID) AS TotalOrders,
            SUM(order_header.SubTotal) AS SumSubTotal
        FROM
            slv_ghq_tech_adventureworks.sales_order_header AS order_header
            INNER JOIN slv_ghq_tech_adventureworks.customer
                ON customer.CustomerID = order_header.CustomerID
            INNER JOIN slv_ghq_tech_adventureworks.address AS ship_to_address
                ON ship_to_address.AddressID = order_header.ShipToAddressID
        WHERE
            order_header.Status NOT IN (
                1, -- In Process
                4, -- Rejected
                6 -- Canceled
            )
        GROUP BY
            OrderYearMonth,
            OnlineOrderFlag,
            CustomerGender,
            ShipToCountryRegion,
            ShipToStateProvince,
            ShipToCity
        WITH ROLLUP
    """)
except Exception:
    common_utils.exit_with_last_exception()

# display(df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(df)

# display(audit_df)

# COMMAND ----------

target_location = lakehouse_utils.generate_gold_table_location(
    lakehouse_gold_root=lakehouse_gold_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    target_data_product=target_data_product,
    database_name=target_database,
    table_name=target_table,
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_delta_table(
    df=audit_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=write_utils.LoadType.OVERWRITE_TABLE,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.OVERWRITE_SCHEMA,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)
