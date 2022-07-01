# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import os
from test_write_utils import *

# COMMAND ----------

from datetime import datetime
current_ts = datetime.strftime(datetime.utcnow(),'%Y%m%d%H%M%S')

# COMMAND ----------

tmpdir = f"/dbfs/tmp/test_scd_2/{current_ts}"
dbutils.fs.rm("dbfs:/tmp/test_scd_2/", recurse=True)
spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
dbutils.fs.mkdirs(tmpdir)

# COMMAND ----------

test_write_scd_type_2_first_write(tmpdir)
print("test_write_scd_type_2_first_write done")

# COMMAND ----------

test_write_scd_type_2_only_new_ids(tmpdir)
print("test_write_scd_type_2_only_new_ids done")

# COMMAND ----------

test_write_scd_type_2_only_updates(tmpdir)
print("test_write_scd_type_2_only_updates done")

# COMMAND ----------

test_write_scd_type_2_same_id_same_data(tmpdir)
print("test_write_scd_type_2_same_id_same_data done")

# COMMAND ----------

test_write_scd_type_2_updates_and_new_records(tmpdir)
print("test_write_scd_type_2_updates_and_new_records done")

# COMMAND ----------

test_write_scd_type_2_multiple_keys(tmpdir)
print("test_write_scd_type_2_multiple_keys done")

# COMMAND ----------

test_write_scd_type_2_schema_evolution(tmpdir)
print("test_write_scd_type_2_schema_evolution done")

# COMMAND ----------

test_write_scd_type_2_partition(tmpdir)
print("test_write_scd_type_2_partition done")

# COMMAND ----------

dbutils.notebook.exit(1)
