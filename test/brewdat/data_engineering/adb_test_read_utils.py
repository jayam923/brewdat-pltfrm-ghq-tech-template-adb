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


# COMMAND ----------

test_read_raw_dataframe_xml_simple(file_location = f"file:{os.getcwd()}/support_files/read_raw_dataframe/xml_simple.xml")

# COMMAND ----------

test_read_raw_dataframe_xml_simple_row_tag(file_location = f"file:{os.getcwd()}/support_files/read_raw_dataframe/xml_simple_tag.xml")

# COMMAND ----------

test_read_raw_dataframe_csv_delimiter(file_location = f"file:{os.getcwd()}/support_files/read_raw_dataframe/csv_delimiter.csv")

# COMMAND ----------

test_read_raw_dataframe_csv_has_headers(file_location=f"file:{os.getcwd()}/support_files/read_raw_dataframe/csv_no_headers.csv")

# COMMAND ----------

test_read_raw_dataframe_excel_simple(file_location=f"file:{os.getcwd()}/support_files/read_raw_dataframe/excel_simple.xlsx")
