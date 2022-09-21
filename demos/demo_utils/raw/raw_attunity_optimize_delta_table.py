# Databricks notebook source
# DBTITLE 1,Define widgets
dbutils.widgets.text("target_database", "brz_ghq_tech_sap_europe", "1 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "kna1", "2 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("partition_column", "TARGET_APPLY_DT", "3 - partition_column")
partition_column = dbutils.widgets.get("partition_column")
print(f"{partition_column = }")

full_table_name = f"`{target_database}`.`{target_table}`"
print(f"{full_table_name = }")

# COMMAND ----------

# DBTITLE 1,Enable autoOptimize and Change Data Feed
spark.sql(f"""
    ALTER TABLE {full_table_name}
    SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true,
        delta.enableChangeDataFeed = true
    )
""")

spark.sql(f"DESCRIBE HISTORY {full_table_name}").display()

# COMMAND ----------

# DBTITLE 1,Optimize delta table
spark.sql(f"OPTIMIZE {full_table_name}").display()

# COMMAND ----------

# DBTITLE 1,Rewrite delta table partitioned by selected column
from pyspark.sql.utils import AnalysisException

try:
    actual_partition_columns = spark.sql(f"SHOW PARTITIONS {full_table_name}").columns
except AnalysisException:
    actual_partition_columns = []
print(f"{actual_partition_columns = }")

if actual_partition_columns != [partition_column]:
    print("Repartitioning...")

    # Find table's external location
    table_location = (
        spark.sql(f"DESCRIBE DETAIL {full_table_name}")
        .select("location")
        .collect()[0][0]
    )
    assert table_location

    # Overwrite with requested partition column
    (
        spark.read
        .format("delta")
        .load(table_location)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy(partition_column)
        .save(table_location)
    )
else:
    print("Repartitioning was not required")
