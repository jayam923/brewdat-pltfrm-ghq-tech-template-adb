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

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_adventureworks", "5 - target_hive_database")
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

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# Gather standard Lakehouse environment variables
environment = os.getenv("ENVIRONMENT")
lakehouse_bronze_root = os.getenv("LAKEHOUSE_BRONZE_ROOT")
lakehouse_silver_root = os.getenv("LAKEHOUSE_SILVER_ROOT")

# Ensure that all standard Lakehouse environment variables are set
if None in [environment, lakehouse_bronze_root, lakehouse_silver_root]:
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

key_columns = ["sales_order_id"]

df = spark.sql("""
        SELECT
            CAST(SalesOrderId AS INT) AS sales_order_id,
            CAST(RevisionNumber AS TINYINT) AS revision_number,
            TO_DATE(OrderDate) AS order_date,
            TO_DATE(DueDate) AS due_date,
            TO_DATE(ShipDate) AS ship_date,
            CAST(Status AS TINYINT) AS status_code,
            CASE
                WHEN Status = 1 THEN 'In Process'
                WHEN Status = 2 THEN 'Approved'
                WHEN Status = 3 THEN 'Backordered'
                WHEN Status = 4 THEN 'Rejected'
                WHEN Status = 5 THEN 'Shipped'
                WHEN Status = 6 THEN 'Canceled'
                WHEN Status IS NULL THEN NULL
                ELSE '--MAPPING ERROR--'
            END AS status,
            CAST(OnlineOrderFlag AS BOOLEAN) AS online_order_flag,
            SalesOrderNumber AS sales_order_number,
            PurchaseOrderNumber AS purchase_order_number,
            AccountNumber AS account_number,
            CAST(CustomerId AS INT) AS customer_id,
            CAST(ShipToAddressId AS INT) AS ship_to_address_id,
            CAST(BillToAddressId AS INT) AS bill_to_address_id,
            ShipMethod AS ship_method,
            CAST(SubTotal AS DECIMAL(19,4)) AS sub_total,
            CAST(TaxAmt AS DECIMAL(19,4)) AS tax_amount,
            CAST(Freight AS DECIMAL(19,4)) AS freight,
            CAST(TotalDue AS DECIMAL(19,4)) AS total_due,
            TO_TIMESTAMP(ModifiedDate) AS modified_date,
            __ref_dt
        FROM
            brz_ghq_tech_adventureworks.sales_order_header
        WHERE 1 = 1
            AND __ref_dt BETWEEN DATE_FORMAT('{data_interval_start}', 'yyyyMMdd')
                AND DATE_FORMAT('{data_interval_end}', 'yyyyMMdd')
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
    watermark_column="__ref_dt",
)

#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

#display(audit_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
