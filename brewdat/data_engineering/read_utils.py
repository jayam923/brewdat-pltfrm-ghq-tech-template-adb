from enum import Enum, unique

import pyspark.pandas as ps
import pyspark.sql.functions as F
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


def read_raw_dataframe_stream(
        spark: SparkSession,
        dbutils: object,
        file_format: RawFileFormat,
        location: str,
        schema_location: str = None,
        cast_all_to_string: bool = True,
        rescue_columns: bool = True,
        csv_has_headers: bool = True,
        csv_delimiter: str = ",",
        csv_escape_character: str = "\"",
        max_bytes_per_trigger: str = "10g",
        max_files_per_trigger: int = 1000,
        use_incremental_listing: str = "true",
        backfill_interval: str = None,  # 1 week, 1 day
        allow_overwrites: bool = False,
        additional_options: dict = {},
) -> DataFrame:
    """Read a streaming DataFrame from the Raw Layer.

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
        schema_location: str, default=None
            Absolute Data Lake path to store the inferred schema and subsequent changes.
            If not informed, the following location is going to be used by default: {location}/_autoloader_schema.
            Format: "abfss://container@storage_account.dfs.core.windows.net/path/to/dataset/_schema".
        rescue_columns: bool, default=True
            Whether to bring back rescued data from columns that had schema mismatches during schema inference.
        cast_all_to_string : bool, default=True
            Whether to cast all non-string values to string.
            Useful to maximize schema compatibility in the Bronze layer.
        csv_has_headers : bool, default=True
            Whether the CSV file has a header row.
        csv_delimiter : str, default=","
            Delimiter string for CSV file format.
        csv_escape_character : str, default="\\""
            Escape character for CSV file format.
        max_bytes_per_trigger: str, default="10g"
            The maximum number of new bytes to be processed in every trigger. You can specify a byte string such
            as 10g to limit each microbatch to 10 GB of data. This is a soft maximum. If you have files that
            are 3 GB each, Databricks processes 12 GB in a microbatch. When used together with
            max_files_per_trigger, Databricks consumes up to the lower limit of max_files_per_trigger or
            max_bytes_per_trigger, whichever is reached first. This option has no effect when used with Trigger.Once().
        max_files_per_trigger: int, default=1000
            The maximum number of new files to be processed in every trigger. When used together with
            max_bytes_per_trigger, Databricks consumes up to the lower limit of max_files_per_trigger or
            max_bytes_per_trigger, whichever is reached first. This option has no effect when used with Trigger.Once().
        use_incremental_listing: str, default="true"
            Whether to use the incremental listing rather than the full listing in directory listing mode.
            With "auto" mode, reading process will make the best effort to automatically detect if a given directory is
            applicable for the incremental listing. You can explicitly use the incremental listing or use the
            full directory listing by setting it as true or false respectively.
            Available values: "auto", "true", "false"
        backfill_interval: str, default=None
            In order to guarantee the eventual completeness, a interval can be set to trigger asynchronous
            backfills at a given interval, e.g. "1 day" to backfill once a day, or "1 week" to backfill once a week.
        allow_overwrites: bool, default=False
            Whether to allow input directory file changes to overwrite existing data.
        additional_options : dict, default={}
            Dictionary with additional options for spark.read.

        Returns
        -------
        DataFrame
            The PySpark streaming DataFrame read from the Raw Layer.
        """
    try:

        spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", False)

        RESCUE_COLUMN = "__rescued_data"

        schema_location = schema_location if schema_location else f"{location}/_autoloader_schema"
        df_reader = (
            spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", file_format.lower())
                .option("cloudFiles.useNotifications", False)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.maxBytesPerTrigger", max_bytes_per_trigger)
                .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger)
                .option("cloudFiles.useIncrementalListing", use_incremental_listing)
                .option("cloudFiles.allowOverwrites", allow_overwrites)
                .option("rescuedDataColumn", RESCUE_COLUMN)
                .option("readerCaseSensitive", False)
                .options(**additional_options)
        )

        if backfill_interval:
            df_reader = df_reader("cloudFiles.backfillInterval", backfill_interval)
            
        if file_format == RawFileFormat.CSV:
            df_reader = (
                df_reader
                .option("mergeSchema", True)
                .option("header", csv_has_headers)
                .option("delimiter", csv_delimiter)
                .option("escape", csv_escape_character)
            )

        df = df_reader.load(location)

        if cast_all_to_string:
            df = transform_utils.cast_all_columns_to_string(dbutils, df)

        # Bring back rescued data from columns that had schema mismatches during schema inference.
        if rescue_columns and file_format != RawFileFormat.CSV and file_format != RawFileFormat.JSON:
            df = _rescue_columns(df=df, rescue_column=RESCUE_COLUMN)
        df = df.drop(RESCUE_COLUMN)

        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def _rescue_columns(
    df: DataFrame, 
    rescue_column: str
) -> DataFrame:
    """
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to modify.
    rescue_column: str
        Nome of the column containing rescued data.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with rescued values.
    """
    schema_str = df.schema.simpleString()
    df = df.withColumn(rescue_column, F.from_json(rescue_column, schema_str))
    for col in df.columns:
        if col == rescue_column:
            continue
        df = df.withColumn(col, F.coalesce(col, f"`{rescue_column}`.`{col}`"))
    return df
    