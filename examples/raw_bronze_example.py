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

df = BrewDatFramework.read_raw_dataframe(
    spark,
    file_format=BrewDatFramework.RawFileFormat.CSV,
    location=f"{BrewDatFramework.LAKEHOUSE_LANDING_ROOT}/data/ghq/tech/adventureworks/adventureworkslt/saleslt/customer2/"
)

# COMMAND ----------

df = BrewDatFramework.clean_column_names(df)

# COMMAND ----------

try:
    from pyspark.sql import functions as F

    df = (
        df
        .filter(F.date_format(F.col("ModifiedDate"), 'yyyyMMdd').between(
            F.date_format(F.lit(watermark_start_datetime), "yyyyMMdd"),
            F.date_format(F.lit(watermark_end_datetime), "yyyyMMdd"),
        ))
    )

except:
    BrewDatFramework.exit_with_last_exception()

# COMMAND ----------

df = BrewDatFramework.create_or_replace_audit_columns(df)

# COMMAND ----------

df = df.withColumn("update_year", F.date_format("ModifiedDate", "yyyy"))
df = df.withColumn("update_month", F.date_format("ModifiedDate", "MM"))
df = df.withColumn("update_day", F.date_format("ModifiedDate", "dd"))

# COMMAND ----------

zone = "ghq"
business_domain = "tech"
source_system = "adventureworks"
table_name = "saleslt_customer2"
schema_name = f"brz_{zone}_{business_domain}_{source_system}"
partition_columns=["update_year", "update_month", "update_day"]

location = BrewDatFramework.generate_bronze_table_location(
    source_zone=zone,
    source_business_domain=business_domain,
    source_system_name=source_system,
    source_dataset=table_name,
)

results = BrewDatFramework.write_delta_table(
    spark=spark,
    df=df,
    location=location,
    schema_name=schema_name,
    table_name=table_name,
    load_type=BrewDatFramework.LoadType.APPEND_ALL,
    partition_columns=partition_columns,
    schema_evolution_mode=BrewDatFramework.SchemaEvolutionMode.ADD_NEW_COLUMNS,
)

print(results)

# COMMAND ----------

BrewDatFramework.exit_with_object(dbutils, results)
