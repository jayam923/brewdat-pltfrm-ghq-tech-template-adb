# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import os

from test_transform_utils import *

# COMMAND ----------

from datetime import datetime
current_ts = datetime.strftime(datetime.utcnow(),'%Y%m%d%H%M%S')

tmpdir = f"/dbfs/tmp/test_transform/{current_ts}"
dbutils.fs.rm("dbfs:/tmp/test_write/", recurse=True)
spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
dbutils.fs.mkdirs(tmpdir)

# COMMAND ----------

test_deduplicate_records_stream_microbatch(tmpdir, f"file:{os.getcwd()}/support_files/read_raw_dataframe/delta_simple1")

# COMMAND ----------

test_clean_column_names()

test_clean_column_names_except_for()

test_flatten_dataframe_no_struct_columns()

test_flatten_dataframe()

test_flatten_dataframe_custom_separator()

test_flatten_dataframe_except_for()

test_flatten_dataframe_recursive()

test_flatten_dataframe_not_recursive()

test_flatten_dataframe_recursive_deeply_nested()

test_flatten_dataframe_recursive_except_for()

test_flatten_dataframe_preserve_columns_order()

test_flatten_dataframe_map_type()

test_flatten_dataframe_array_explode_disabled()

test_flatten_dataframe_array()

test_flatten_dataframe_array_of_structs()

test_flatten_dataframe_misc_data_types()

test_create_or_replace_audit_columns()

test_create_or_replace_audit_columns_already_exist()

test_create_or_replace_business_key_column()

test_business_key_column_no_key_provided()

test_business_key_column_keys_are_null()
