from enum import Enum, unique
from typing import Any

import pyspark.pandas as ps
from pyspark.sql import DataFrame, SparkSession

from . import common_utils, transform_utils
from .common_utils import with_exception_handling


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


@with_exception_handling
def read_raw_dataframe(
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
        This may be used to override default options set by this function.

    Returns
    -------
    DataFrame
        The PySpark DataFrame read from the Raw Layer.
    """
    # Read DataFrame
    spark = SparkSession.getActiveSession()
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

    # Apply Bronze transformations
    if cast_all_to_string:
        df = transform_utils.cast_all_columns_to_string(df)

    return df


@with_exception_handling
def read_raw_streaming_dataframe(
    file_format: RawFileFormat,
    location: str,
    schema_location: str = None,
    handle_rescued_data: bool = True,
    cast_all_to_string: bool = True,
    csv_has_headers: bool = True,
    csv_delimiter: str = ",",
    csv_escape_character: str = "\"",
    json_is_multiline: bool = True,
    additional_options: dict = {},
) -> DataFrame:
    """Read a streaming DataFrame from the Raw Layer.

    Parameters
    ----------
    file_format : RawFileFormat
        The raw file format use in this dataset (CSV, PARQUET, etc.).
    location : str
        Absolute Data Lake path for the physical location of this dataset.
        Format: "abfss://container@storage_account.dfs.core.windows.net/path/to/dataset/".
    schema_location : str, default=None
        Absolute Data Lake path to store the inferred schema and subsequent changes.
        If not informed, the following location is going to be used by default: {location}/_schema.
        Format: "abfss://container@storage_account.dfs.core.windows.net/path/to/schema/folder".
    handle_rescued_data : bool, default=True
        Whether to bring back rescued data from columns that had schema mismatches during schema
        inference. This is only possible if the offending columns are first cast to string.
    cast_all_to_string : bool, default=True
        Whether to cast all non-string values to string.
        Useful to maximize schema compatibility in the Bronze layer.
    csv_has_headers : bool, default=True
        Whether the CSV file has a header row.
    csv_delimiter : str, default=","
        Delimiter string for CSV file format.
    csv_escape_character : str, default="\\""
        Escape character for CSV file format.
    json_is_multiline : bool, default=True
        Set to True when JSON file has a single record spanning several lines.
        Set to False when JSON file has one record per line (JSON Lines format).
    additional_options : dict, default={}
        Dictionary with additional options for spark.readStream.
        This may be used to override default options set by this function.

    Returns
    -------
    DataFrame
        The PySpark streaming DataFrame read from the Raw Layer.
    """
    RESCUE_COLUMN = "__rescued_data"

    # Create streaming DataFrame
    spark = SparkSession.getActiveSession()
    if file_format in [RawFileFormat.EXCEL, RawFileFormat.XML]:
        # Unsupported formats
        raise NotImplementedError
    elif file_format == RawFileFormat.DELTA:
        # Read using Delta Streaming
        df = (
            spark.readStream
            .format("delta")
            .option("ignoreChanges", True)  # reprocess updates to old files
            .option("maxBytesPerTrigger", "10g")
            .option("maxFilesPerTrigger", 1000)
            .load(location)
        )
    else:
        # Disable inclusion of filename in the rescue data column
        spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", False)

        # Read using Auto Loader
        df_reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format.lower())
            .option("cloudFiles.useNotifications", False)
            .option("cloudFiles.useIncrementalListing", "auto")
            .option("cloudFiles.allowOverwrites", True)  # reprocess updates to old files
            .option("cloudFiles.maxBytesPerTrigger", "10g")
            .option("cloudFiles.maxFilesPerTrigger", 1000)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", schema_location or location)
            .option("rescuedDataColumn", RESCUE_COLUMN)
            .option("readerCaseSensitive", False)
            .options(**additional_options)
        )

        if file_format == RawFileFormat.CSV:
            df_reader = (
                df_reader
                .option("header", csv_has_headers)
                .option("delimiter", csv_delimiter)
                .option("escape", csv_escape_character)
            )
        elif file_format == RawFileFormat.JSON:
            df_reader = df_reader.option("multiLine", json_is_multiline)

        df = df_reader.load(location)

    # Apply Bronze transformations
    if cast_all_to_string:
        df = transform_utils.cast_all_columns_to_string(df)

        # Bring back rescued data from columns that had schema mismatches during schema inference
        # Not necessary for CSV and JSON formats, as they are inferred as string
        # Also not necessary for Delta, as it leverages Delta Streaming instead
        if handle_rescued_data and RESCUE_COLUMN in df.columns:
            df = transform_utils.handle_rescued_data(df, RESCUE_COLUMN)

    return df
