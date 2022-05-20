# Databricks notebook source
import sys
sys.path.append("/Workspace/Repos/brewdat_framework/[version]")

# COMMAND ----------

from brewdat.data_engineering.utils import BrewDatFramework

# COMMAND ----------

dbutils.widgets.text("watermark_start_datetime", "2005-01-01 00:00:00")
watermark_start_datetime = dbutils.widgets.get("watermark_start_datetime")
print(f"watermark_start_datetime: {watermark_start_datetime}")

dbutils.widgets.text("watermark_end_datetime", "2005-12-31 23:59:59")
watermark_end_datetime = dbutils.widgets.get("watermark_end_datetime")
print(f"watermark_end_datetime: {watermark_end_datetime}")

# COMMAND ----------

try:
    from pyspark.sql import functions as F

    df = (
      spark.sql(f"""
        SELECT
            *
        FROM brz_ghq_tech_adventureworks.saleslt_customer2
        WHERE 1 = 1
        AND reference_year BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyy') AND DATE_FORMAT('{watermark_end_datetime}', 'yyyy') 
        AND reference_month BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyyMM') AND DATE_FORMAT('{watermark_end_datetime}', 'yyyyMM')
        AND reference_day BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyyMMdd') AND DATE_FORMAT('{watermark_end_datetime}', 'yyyyMMdd')
      """)
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    )

except:
    BrewDatFramework.exit_with_last_exception()

# COMMAND ----------

key_columns = ["CustomerID"]

df = BrewDatFramework.deduplicate_records(
    df=df,
    key_columns=key_columns,
    watermark_column="ModifiedDate",
)

# COMMAND ----------

df = BrewDatFramework.create_or_replace_audit_columns(df)

# COMMAND ----------

zone="ghq"
business_domain="tech"
source_system="adventureworks"

schema_name = f"brz_{zone}_{business_domain}_{source_system}"
table_name = "saleslt_customer2"

partition_columns=[]

location = BrewDatFramework.generate_silver_table_location(
    source_business_domain=business_domain,
    source_zone=zone,
    source_system_name=source_system,
    table_name=table_name,
)

results = BrewDatFramework.write_delta_table(
    spark=spark,
    df=df,
    location=location,
    schema_name=schema_name,
    table_name=table_name,
    load_type=BrewDatFramework.LoadType.UPSERT,
    key_columns=key_columns,
    partition_columns=partition_columns,
    schema_evolution_mode=BrewDatFramework.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(results)

# COMMAND ----------

BrewDatFramework.exit_with_object(dbutils, results)
