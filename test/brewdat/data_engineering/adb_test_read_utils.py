# Databricks notebook source
import os

from test_read_utils import *

# COMMAND ----------

test_read_raw_dataframe_csv_simple(f"file:{os.getcwd()}/support_files/read_raw_dataframe/csv_simple1.csv")

test_read_raw_dataframe_parquet_simple(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_simple1.parquet")

test_read_raw_dataframe_orc_simple(f"file:{os.getcwd()}/support_files/read_raw_dataframe/orc_simple1.orc")

test_read_raw_dataframe_delta_simple(f"file:{os.getcwd()}/support_files/read_raw_dataframe/delta_simple1")

test_read_raw_dataframe_parquet_with_array(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_array.parquet")

test_read_raw_dataframe_parquet_with_struct(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_struct.parquet")

test_read_raw_dataframe_parquet_with_deeply_nested_struct(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct.parquet")

test_read_raw_dataframe_parquet_with_array_of_struct(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_array_of_struct.parquet")

test_read_raw_dataframe_parquet_with_deeply_nested_struct_inside_array(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct_inside_array.parquet")

test_read_raw_dataframe_parquet_with_deeply_nested_struct_inside_array_do_not_cast_types(f"file:{os.getcwd()}/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct_inside_array.parquet")

