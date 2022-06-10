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


class DataFrameTransform:
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

        
    def clean_column_names(
        self,
        df: DataFrame,
        except_for: List[str] = [],
    )-> DataFrame:
        """Normalize the name of all the columns in a given DataFrame.

        Uses BrewDat's standard approach as seen in other Notebooks.
        Improved to also trim (strip) whitespaces.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        except_for : List[str], default=[]
            A list of column names that should NOT be modified.

        Returns
        -------
        DataFrame
            The modified PySpark DataFrame with renamed columns.
        """
        try:
            column_names = df.schema.names
            for column_name in column_names:
                if column_name in except_for:
                    continue  # Skip

                # \W is "anything that is not alphanumeric or underscore"
                # Equivalent to [^A-Za-z0-9_]
                new_column_name = re.sub("\W+", "_", column_name.strip())
                if column_name != new_column_name:
                    df = df.withColumnRenamed(column_name, new_column_name)
            return df
        except:
            self.exit_with_last_exception()
         
       
    def create_or_replace_business_key_column(
        self,
        df: DataFrame,
        business_key_column_name: str,
        key_columns: List[str],
        separator: str = "__",
        check_null_values: bool = True,
    ) -> DataFrame:
        """Create a standard business key concatenating multiple columns.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        business_key_column_name : str
            The name of the concatenated business key column.
        key_columns : List[str]
            The names of the columns used to uniquely identify each record the table.
        separator : str, default="__"
            A string to separate the values of each column in the business key.
        check_null_values : bool, default=True
            Whether to check if the given key columns contain NULL values.
            Throw an error if any NULL value is found.

        Returns
        -------
        DataFrame
            The PySpark DataFrame with the desired business key.
        """
        try:
            if not key_columns:
                raise ValueError("No key column was given")

            if check_null_values:
                filter_clauses = [f"`{key_column}` IS NULL" for key_column in key_columns]
                filter_string = " OR ".join(filter_clauses)
                if df.filter(filter_string).limit(1).count() > 0:
                    # TODO: improve error message
                    raise ValueError("Business key would contain null values.")

            df = df.withColumn(business_key_column_name, F.lower(F.concat_ws(separator, *key_columns)))

            return df

        except:
            self.exit_with_last_exception()

 
    def create_or_replace_audit_columns(self, df: DataFrame) -> DataFrame:
        """Create or replace BrewDat audit columns in the given DataFrame.

        The following audit columns are created/replaced:
            - __insert_gmt_ts: timestamp of when the record was inserted.
            - __update_gmt_ts: timestamp of when the record was last updated.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.

        Returns
        -------
        DataFrame
            The modified PySpark DataFrame with audit columns.
        """
        try:
            # Get current timestamp
            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Create or replace columns
            if "__insert_gmt_ts" in df.columns:
                df = df.fillna(current_timestamp, "__insert_gmt_ts")
            else:
                df = df.withColumn("__insert_gmt_ts", F.lit(current_timestamp).cast("timestamp"))
            df = df.withColumn("__update_gmt_ts", F.lit(current_timestamp).cast("timestamp"))
            return df

        except:
            self.exit_with_last_exception()

            
    def deduplicate_records(
        self,
        df: DataFrame,
        key_columns: List[str],
        watermark_column: str,
    ) -> DataFrame:
        """Deduplicate rows from a DataFrame using key and watermark columns.

        We do not use orderBy followed by dropDuplicates because it
        would require a coalesce(1) to preserve the order of the rows.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        key_columns : List[str]
            The names of the columns used to uniquely identify each record the table.
        watermark_column : str
            The name of a datetime column used to select the newest records.

        Returns
        -------
        DataFrame
            The deduplicated PySpark DataFrame.
        """
        try:
            if not key_columns:
                raise ValueError("No key column was given")

            return (
                df
                .withColumn("__dedup_row_number", F.row_number().over(
                    Window.partitionBy(*key_columns).orderBy(F.col(watermark_column).desc())
                ))
                .filter("__dedup_row_number = 1")
                .drop("__dedup_row_number")
            )

        except:
            self.exit_with_last_exception()