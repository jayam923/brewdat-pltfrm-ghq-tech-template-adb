# Databricks notebook source
import os

from test_write_utils import *

# COMMAND ----------

test_recreate_check(schema_name = "brz_ghq_tech_adventureworks",
    table_name = "customer_new_4",
    location = 'abfss://bronze@brewdatpltfrmrawbrzd.dfs.core.windows.net/data/ghq/tech/adventureworks/customer_new_3',
    assert_check = True                
    )
