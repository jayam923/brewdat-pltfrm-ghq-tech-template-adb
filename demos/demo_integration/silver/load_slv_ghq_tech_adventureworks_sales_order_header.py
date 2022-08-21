# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "1 - brewdat_library_version")
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

import sys

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, data_quality_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(transform_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[adls_raw_bronze_storage_account_name, adls_silver_gold_storage_account_name],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

from pyspark.sql import functions as F

try:
    key_columns = ["SalesOrderID"]

    bronze_df = (
        spark.read
        .table("brz_ghq_tech_adventureworks.sales_order_header")
        .filter(F.col("__ref_dt").between(
            F.date_format(F.lit(data_interval_start), "yyyyMMdd"),
            F.date_format(F.lit(data_interval_end), "yyyyMMdd")
        ))
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)

#display(bronze_df)

# COMMAND ----------

bronze_dq_df = (
    data_quality_utils.DataQualityChecker(dbutils=dbutils, df=bronze_df)
    .check_column_is_not_null(column_name="SalesOrderID")
    .check_column_is_not_null(column_name="CustomerID")
    .check_column_type_cast(column_name="SalesOrderID", data_type="int")
    .check_column_type_cast(column_name="RevisionNumber", data_type="tinyint")
    .check_column_type_cast(column_name="OrderDate", data_type="date")
    .check_column_type_cast(column_name="DueDate", data_type="date")
    .check_column_type_cast(column_name="ShipDate", data_type="date")
    .check_column_type_cast(column_name="Status", data_type="tinyint")
    .check_column_type_cast(column_name="OnlineOrderFlag", data_type="boolean")
    .check_column_type_cast(column_name="CustomerID", data_type="int")
    .check_column_type_cast(column_name="ShipToAddressID", data_type="int")
    .check_column_type_cast(column_name="BillToAddressID", data_type="int")
    .check_column_type_cast(column_name="SubTotal", data_type="decimal(19,4)")
    .check_column_type_cast(column_name="TaxAmt", data_type="decimal(19,4)")
    .check_column_type_cast(column_name="Freight", data_type="decimal(19,4)")
    .check_column_type_cast(column_name="TotalDue", data_type="decimal(19,4)")
    .check_column_type_cast(column_name="ModifiedDate", data_type="timestamp")
    .check_column_value_is_in(column_name="Status", valid_values=[1, 2, 3, 4, 5, 6])
    .check_column_max_length(column_name="SalesOrderNumber", maximum_length=30)
    .check_column_max_length(column_name="PurchaseOrderNumber", maximum_length=30)
    .check_column_max_length(column_name="ShipMethod", maximum_length=100)
    .check_column_max_length(column_name="AccountNumber", maximum_length=15)
    .check_column_matches_regular_expression(column_name="AccountNumber", regular_expression="^\d{2}-\d{4}-\d{6}$")
    .build_df()
)

bronze_dq_df.createOrReplaceTempView("v_bronze_dq_df")

#display(bronze_dq_df)

# COMMAND ----------

transformed_df = spark.sql("""
    SELECT
        CAST(SalesOrderID AS INT) AS SalesOrderID,
        CAST(RevisionNumber AS TINYINT) AS RevisionNumber,
        TO_DATE(OrderDate) AS OrderDate,
        TO_DATE(DueDate) AS DueDate,
        TO_DATE(ShipDate) AS ShipDate,
        CAST(Status AS TINYINT) AS Status,
        CASE
            WHEN Status = 1 THEN 'In Process'
            WHEN Status = 2 THEN 'Approved'
            WHEN Status = 3 THEN 'Backordered'
            WHEN Status = 4 THEN 'Rejected'
            WHEN Status = 5 THEN 'Shipped'
            WHEN Status = 6 THEN 'Canceled'
            WHEN Status IS NULL THEN NULL
            ELSE '--MAPPING ERROR--'
        END AS StatusDescription,
        CAST(OnlineOrderFlag AS BOOLEAN) AS OnlineOrderFlag,
        SalesOrderNumber AS SalesOrderNumber,
        PurchaseOrderNumber AS PurchaseOrderNumber,
        AccountNumber AS AccountNumber,
        CAST(CustomerID AS INT) AS CustomerID,
        CAST(ShipToAddressID AS INT) AS ShipToAddressID,
        CAST(BillToAddressID AS INT) AS BillToAddressID,
        ShipMethod,
        CAST(SubTotal AS DECIMAL(19,4)) AS SubTotal,
        CAST(TaxAmt AS DECIMAL(19,4)) AS TaxAmt,
        CAST(Freight AS DECIMAL(19,4)) AS Freight,
        CAST(TotalDue AS DECIMAL(19,4)) AS TotalDue,
        TO_TIMESTAMP(ModifiedDate) AS ModifiedDate,
        __data_quality_issues
    FROM
        v_bronze_dq_df
""")

#display(transformed_df)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=transformed_df,
    key_columns=key_columns,
    watermark_column="ModifiedDate",
)

#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

#display(audit_df)

# COMMAND ----------

silver_dq_df = (
    data_quality_utils.DataQualityChecker(dbutils=dbutils, df=audit_df)
    .check_composite_column_value_is_unique(column_names=key_columns)
    .check_column_value_is_not_in(column_name="StatusDescription", invalid_values=["--MAPPING ERROR--"])
    .check_narrow_condition(
        expected_condition="TotalDue - SubTotal - TaxAmt - Freight < 0.01",
        failure_message="CHECK_TOTAL_DUE: TotalDue should be equal to SubTotal + TaxAmt + Freight",
    )
    .build_df()
)

#display(silver_dq_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(f"location: {location}")

# COMMAND ----------

results = write_utils.write_delta_table(
    spark=spark,
    df=silver_dq_df,
    location=location,
    database_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
