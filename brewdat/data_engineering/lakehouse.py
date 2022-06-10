import json
import os
import re
import sys
import traceback

import pyspark.sql.functions as F

from datetime import datetime
from enum import Enum, unique
from typing import List, TypedDict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


class DataFrameLakehouse: 
    """Reusable functions for all BrewDat projects.

    Attributes
    ----------
    spark : SparkSession
        A Spark session.
    """

   
    ##################
    # Public methods #
    ##################

    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        
    def generate_bronze_table_location(
        self,
        lakehouse_bronze_root: str,
        target_zone: str,
        target_business_domain: str,
        source_system: str,
        table_name: str,
    ) -> str:
        """Build the standard location for a Bronze table.

        Parameters
        ----------
        lakehouse_bronze_root : str
            Root path to the Lakehouse's Bronze layer.
            Format: "abfss://bronze@storage_account.dfs.core.windows.net".
            Value varies by environment, so you should use environment variables.
        target_zone : str
            Zone of the target dataset.
        target_business_domain : str
            Business domain of the target dataset.
        source_system : str
            Name of the source system.
        table_name : str
            Name of the target table in the metastore.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [lakehouse_bronze_root, target_zone, target_business_domain, source_system, table_name]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            return f"{lakehouse_bronze_root}/data/{target_zone}/{target_business_domain}/{source_system}/{table_name}".lower()

        except:
            self.exit_with_last_exception()


    def generate_silver_table_location(
        self,
        lakehouse_silver_root: str,
        target_zone: str,
        target_business_domain: str,
        source_system: str,
        table_name: str,
    ) -> str:
        """Build the standard location for a Silver table.

        Parameters
        ----------
        lakehouse_silver_root : str
            Root path to the Lakehouse's Silver layer.
            Format: "abfss://silver@storage_account.dfs.core.windows.net".
            Value varies by environment, so you should use environment variables.
        target_zone : str
            Zone of the target dataset.
        target_business_domain : str
            Business domain of the target dataset.
        source_system : str
            Name of the source system.
        table_name : str
            Name of the target table in the metastore.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [lakehouse_silver_root, target_zone, target_business_domain, source_system, table_name]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            return f"{lakehouse_silver_root}/data/{target_zone}/{target_business_domain}/{source_system}/{table_name}".lower()

        except:
            self.exit_with_last_exception()


    def generate_gold_table_location(
        self,
        lakehouse_gold_root: str,
        target_zone: str,
        target_business_domain: str,
        project: str,
        database_name: str,
        table_name: str,
    ) -> str:
        """Build the standard location for a Gold table.

        Parameters
        ----------
        lakehouse_gold_root : str
            Root path to the Lakehouse's Gold layer.
            Format: "abfss://gold@storage_account.dfs.core.windows.net".
            Value varies by environment, so you should use environment variables.
        target_zone : str
            Zone of the target dataset.
        target_business_domain : str
            Business domain of the target dataset.
        project : str
            Project of the target dataset.
        database_name : str
            Name of the target database for the table in the metastore.
        table_name : str
            Name of the target table in the metastore.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [lakehouse_gold_root, target_zone, target_business_domain, project, database_name, table_name]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            return f"{lakehouse_gold_root}/data/{target_zone}/{target_business_domain}/{project}/{database_name}/{table_name}".lower()

        except:
            self.exit_with_last_exception()