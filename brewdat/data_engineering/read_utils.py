from enum import Enum, unique

import pyspark.pandas as ps
from pyspark.sql import DataFrame, SparkSession

from . import common_utils, transform_utils


@unique
class RawFileFormat(str, Enum):
    """Supported raw file formats.
    """
    AVRO = "AVRO"
    """Avro format."""
    CSV = "CSV"
    """Delimited text format."""
    DELTA = "DELTA"
    """Delta format."""
    EXCEL = "EXCEL"
    """EXCEL formats."""
    JSON = "JSON"
    """JSON format."""
    PARQUET = "PARQUET"
    """Parquet format."""
    ORC = "ORC"
    """ORC format."""
    XML = "XML"
    """XML format."""


def read_raw_dataframe(
    spark: SparkSession,
    dbutils: object,
    file_format: RawFileFormat,
    location: str,
    cast_all_to_string: bool = True,
    csv_has_headers: bool = True,
    csv_delimiter: str = ",",
    csv_escape_character: str = "\"",
    excel_sheet_name: str = None,
    excel_has_headers: bool = True,
    json_is_multiline: bool = True,
    xml_row_tag: str = "row",
    additional_options: dict = {},
) -> DataFrame:
    """Read a DataFrame from the Raw Layer.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    dbutils : object
        A Databricks utils object.
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
    excel_sheet_name : str
        Sheet name for EXCEL file format.
        Use None to get all sheets.
    excel_has_headers : bool, default=True
        Whether the Excel file has a header row.
    json_is_multiline : bool, default=True
        Set to True when JSON file has a single record spanning several lines.
        Set to False when JSON file has one record per line (JSON Lines format).
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
        if file_format == RawFileFormat.CSV:
            df = (
                spark.read
                .option("mergeSchema", True)
                .option("header", csv_has_headers)
                .option("delimiter", csv_delimiter)
                .option("escape", csv_escape_character)
                .options(**additional_options)
                .csv(location)
            )
        elif file_format == RawFileFormat.EXCEL:
            psdf = ps.read_excel(
                location,
                excel_sheet_name,
                header=(0 if excel_has_headers else None),
            )
            df = psdf.to_spark()
        elif file_format == RawFileFormat.JSON:
            df = (
                spark.read
                .option("mergeSchema", True)
                .option("multiLine", json_is_multiline)
                .options(**additional_options)
                .json(location)
            )
        elif file_format == RawFileFormat.XML:
            df = (
                spark.read
                .format("xml")
                .option("mergeSchema", True)
                .option("attributePrefix", "")
                .option("valueTag", "value")
                .option("rowTag", xml_row_tag)
                .options(**additional_options)
                .load(location)
            )
        else:
            df = (
                spark.read
                .format(file_format.lower())
                .option("mergeSchema", True)
                .options(**additional_options)
                .load(location)
            )

        if cast_all_to_string:
            df = transform_utils.cast_all_columns_to_string(dbutils, df)

        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)
