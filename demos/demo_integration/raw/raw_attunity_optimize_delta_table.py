# Databricks notebook source
dbutils.widgets.text("source_hive_database", "brz_ghq_tech_sap_europe", "1 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"{source_hive_database = }")

dbutils.widgets.text("source_hive_table", "kna1", "2 - source_hive_table")
source_hive_table = dbutils.widgets.get("source_hive_table")
print(f"{source_hive_table = }")

dbutils.widgets.text("partition_column", "TARGET_APPLY_DT", "3 - partition_column")
partition_column = dbutils.widgets.get("partition_column")
print(f"{partition_column = }")

# COMMAND ----------

# Enable autoOptimize and Change Data Feed options
full_table_name = f"`{source_hive_database}`.`{source_hive_table}`"
spark.sql(f"""
    ALTER TABLE {full_table_name}
    SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true,
        delta.enableChangeDataFeed = true
    )
""")

# Show results
spark.sql(f"DESCRIBE HISTORY {full_table_name}").display()

# COMMAND ----------

# Optimize delta table
results = spark.sql(f"OPTIMIZE {full_table_name}")
display(results)

# COMMAND ----------

partitions_result = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()[0]
partition_col_name = partitions_result.__fields__[0]
print(partition_col_name)

# COMMAND ----------

temp_loc = "abfss://bronze@brewdatpltfrmrawbrzd.dfs.core.windows.net/data/ghq/tech/adventureworks/sales_order_header_testing"
if partition_col_name != partition_column:
    temp_rec = spark.read.format("delta").load(f"{brewdat_ghq_root}/{attunity_sap_prelz_root}_{source_table}__ct")
    temp_rec.write.format("delta")\
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .partitionBy("TARGET_APPLY_DT") \
          .save(temp_loc)
else:
    print("already partitioned by TARGET_APPLY_DT")
