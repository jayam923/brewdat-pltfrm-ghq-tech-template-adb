from enum import Enum, unique

from pyspark.sql import DataFrame, SparkSession

from . import common_utils, transform_utils


@unique
class RawFileFormat(str, Enum):
    """Supported raw file formats.

    AVRO: Avro format.
    CSV: Delimited text format.
    DELTA: Delta format.
    JSON: JSON format.
    PARQUET: Parquet format.
    ORC: ORC format.
    XML: XML format.
    """
    AVRO = "AVRO"
    CSV = "CSV"
    DELTA = "DELTA"
    JSON = "JSON"
    PARQUET = "PARQUET"
    ORC = "ORC"
    XML = "XML"


def read_raw_dataframe(
    spark: SparkSession,
    dbutils: object,
    file_format: RawFileFormat,
    location: str,
    cast_all_to_string: bool = True,
    csv_has_headers: bool = True,
    csv_delimiter: str = ",",
    csv_escape_character: str = "\"",
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
        df_reader = (
            spark.read
            .format(file_format.lower())
            .option("mergeSchema", True)
        )

        if file_format == RawFileFormat.CSV:
            df_reader = (
                df_reader
                .option("header", csv_has_headers)
                .option("delimiter", csv_delimiter)
                .option("escape", csv_escape_character)
            )
        elif file_format == RawFileFormat.XML:
            df_reader = (
                df_reader
                .option("rowTag", xml_row_tag)
            )

        df = (
            df_reader
            .options(**additional_options)
            .load(location)
        )

        if cast_all_to_string:
            df = transform_utils.cast_all_columns_to_string(dbutils, df)

        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)
