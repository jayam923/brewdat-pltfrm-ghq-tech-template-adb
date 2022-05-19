# Databricks notebook source
import sys

sys.path.append("/Workspace/Repos/brewdat_framework/v0.0.1")

# COMMAND ----------

dbutils.widgets.text("watermark_start_datetime", "2022-03-01 00:00:00")
watermark_start_datetime = dbutils.widgets.get("watermark_start_datetime")
print(f"watermark_start_datetime: {watermark_start_datetime}")

dbutils.widgets.text("watermark_end_datetime", "2022-03-30 23:59:59")
watermark_end_datetime = dbutils.widgets.get("watermark_end_datetime")
print(f"watermark_end_datetime: {watermark_end_datetime}")

# COMMAND ----------

def check_workspace_env():
    return 'dev'

from brewdat.data_engineering.utils import BrewDatFramework

BrewDatFramework.LAKEHOUSE_LANDING_ROOT
#abfss://raw@brewdatpltfrmrawbrzd.dfs.core.windows.net/data/
#abfss://raw@brewdatbeesrawbrzd.dfs.core.windows.net'

# COMMAND ----------

df = BrewDatFramework.read_raw_dataframe(
    file_format=BrewDatFramework.RawFileFormat.CSV,
    location=f"{BrewDatFramework.LAKEHOUSE_LANDING_ROOT}/ghq/tech/adventureworks/adventureworkslt/saleslt/customer2/"
)

# COMMAND ----------

df = Framework.clean_column_names(df)


# COMMAND ----------

BrewDatFramework.generate_bronze_table_location(
    source_business_domain='tech', 
    source_dataset='saleslt', 
    source_system_name='adventureworkslt',
    source_zone='ghq'
)

# COMMAND ----------

BrewDatFramework.generate_silver_table_location(
   source_zone = 'ghq',
   source_business_domain = 'tech',
   source_system_name = 'test',
   table_name = 'test'
)
