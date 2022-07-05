# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import os

from test_transform_utils import *

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
