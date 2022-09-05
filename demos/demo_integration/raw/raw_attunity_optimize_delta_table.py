# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.4.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "sap_europe", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("source_table", "KNA1", "03 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"source_table: {source_table}")

dbutils.widgets.text("target_zone", "ghq", "04 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "05 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "brz_ghq_tech_sap_europe", "06 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "kna1", "07 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("data_interval_start", "2022-08-05 00:00:00.000000", "08 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, transform_utils, write_utils

# Print a module's help
help(common_utils)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[
        adls_raw_bronze_storage_account_name,
        adls_brewdat_ghq_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

sap_sid = source_system_to_sap_sid.get(source_system)
attunity_sap_prelz_root = f"/attunity_sap/attunity_sap_{sap_sid}_prelz/prelz_sap_{sap_sid}"
print(f"attunity_sap_prelz_root: {attunity_sap_prelz_root}")

# COMMAND ----------

try:
    ct_df = (
        spark.read
        .format("delta")
        .load(f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}__ct")
    )

except Exception:
    common_utils.exit_with_last_exception(dbutils=dbutils)



# COMMAND ----------

partitions_result = spark.sql(f"SHOW PARTITIONS brz_ghq_tech_adventureworks.sales_order_header_testing").collect()[0]
partition_col_name = partitions_result.__fields__[0]
print(partition_col_name)

# COMMAND ----------

temp_loc = "abfss://bronze@brewdatpltfrmrawbrzd.dfs.core.windows.net/data/ghq/tech/adventureworks/sales_order_header_testing"
if partition_col_name != "TARGET_APPLY_DT" :
    temp_rec = spark.read.format("delta").load(f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}__ct")
    temp_rec.write.format("delta")\
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .partitionBy("TARGET_APPLY_DT") \
          .save(temp_loc)
else :
    print("already partitioned by TARGET_APPLY_DT")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE brz_ghq_tech_adventureworks.sales_order_header_testing SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
