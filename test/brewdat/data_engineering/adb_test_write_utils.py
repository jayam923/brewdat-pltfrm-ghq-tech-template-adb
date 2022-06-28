# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import os

from test_write_utils import *


# COMMAND ----------

location = "dbfs:/tmp/"
test_write_scd_type_2_multiple_keys(location)
#dbutils.fs.rm("dbfs:/tmp/test_write_scd_type_2_updates_and_new_records/",recurse=True)
#spark.sql("drop table if exists test_schema.test_write_scd_type_2_updates_and_new_records")
