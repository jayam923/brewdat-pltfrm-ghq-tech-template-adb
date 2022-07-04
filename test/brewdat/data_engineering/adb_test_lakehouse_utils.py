# Databricks notebook source
import os

from test_lakehouse_utils import *

# COMMAND ----------

test_check_table_name_valid_names()

test_check_table_name_invalid_name1()

test_check_table_name_invalid_name2()

test_check_table_name_invalid_name3()

test_generate_bronze_table_location()

test_generate_silver_table_location()

test_generate_gold_table_location()
