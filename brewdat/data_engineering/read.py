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


class DataFrameRead: 
    """Reusable functions for all BrewDat projects.

    Attributes
    ----------
    spark : SparkSession
        A Spark session.
    """
    

    ########################################
    # Constants, enums, and helper classes #
    ########################################


    @unique
    class RawFileFormat(str, Enum):
        """Supported raw file formats.

        AVRO: Avro format.
        CSV: Delimited text format.
        DELTA: Delta format.
        JSON: JSON format.
        PARQUET: Parquet format.
        ORC: ORC format.
        """
        CSV = "CSV"
        DELTA = "DELTA"
        PARQUET = "PARQUET"
        ORC = "ORC"


    ##################
    # Public methods #
    ##################

    
    def __init__(self, spark: SparkSession):
        self.spark = spark


    def read_raw_dataframe(
        self,
        file_format: RawFileFormat,
        location: str,
        cast_all_to_string: bool = True,
        csv_has_headers: bool = True,
        csv_delimiter: str = ",",
        csv_escape_character: str = "\"",
        xml_row_tag: str = "row",
        additional_options: dict = {},
    ) -> DataFrame:
        """Read a DataFrame from the Raw Layer. Convert all data types to string.

        Parameters
        ----------
        file_format : RawFileFormat
            The raw file format use in this dataset (CSV, PARQUET, etc.).
        location : str
            Absolute Data Lake path for the physical location of this dataset.
            Format: "abfss://container@storage_account.dfs.core.windows.net/path/to/dataset/".
        cast_all_to_string : bool, default=True
            Whether to cast all non-string values to string.
            Useful to maximize schema compatibility in the Bronze layer.
        csv_has_headers : bool, default=True
            Whether the CSV file has a header row.
        csv_delimiter : str, default=","
            Delimiter string for CSV file format.
        csv_escape_character : str, default="\\""
            Escape character for CSV file format.
        xml_row_tag : str, default="row"
            Name of the XML tag to treat as DataFrame rows.
        additional_options : dict, default={}
            Dictionary with additional options for spark.read.

        Returns
        -------
        DataFrame
            The PySpark DataFrame read from the Raw Layer.
        """
        try:
            df = (
                self.spark.read
                .format(file_format.lower())
                .option("mergeSchema", True)
                .option("header", csv_has_headers)
                .option("delimiter", csv_delimiter)
                .option("escape", csv_escape_character)
                .option("rowTag", xml_row_tag)
                .options(**additional_options)
                .load(location)
            )

            if cast_all_to_string:
                # TODO: improve handling of nested types (array, map, struct)
                non_string_columns = [col for col, dtype in df.dtypes if dtype != "string"]
                for column in non_string_columns:
                    df = df.withColumn(column, F.col(column).cast("string"))

            return df

        except:
            self.exit_with_last_exception()
            
  