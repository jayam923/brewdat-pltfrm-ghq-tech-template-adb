import re
from datetime import datetime
from enum import Enum, unique
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DataType

from . import common_utils
from .common_utils import ColumnMapping


@unique
class UnmappedColumnBehavior(str, Enum):
    """Specifies the way in which unmapped DataFrame columns should
    be handled in apply_column_mappings() function.
    """
    IGNORE_UNMAPPED_COLUMNS = "IGNORE_UNMAPPED_COLUMNS"
    """Ignore unmapped columns in the column mappings."""
    FAIL_ON_UNMAPPED_COLUMNS = "FAIL_ON_UNMAPPED_COLUMNS"
    """Raise exception when input columns are missing from
    the column mappings."""


def clean_column_names(
    dbutils: object,
    df: DataFrame,
    except_for: List[str] = [],
) -> DataFrame:
    """Normalize the name of all the columns in a given DataFrame.

    Replaces non-alphanumeric characters with underscore and
    strips leading/trailing underscores, except in metadata columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
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
        for column in df.schema:
            if column.name in except_for:
                continue  # Skip

            new_column_name = _clean_column_name(column.name)
            if column.name != new_column_name:
                df = df.withColumnRenamed(column.name, new_column_name)

            if column.dataType.typeName() in ["array", "map", "struct"]:
                new_data_type = _spark_type_clean_field_names_recurse(column.dataType)
                df = df.withColumn(new_column_name, F.col(new_column_name).cast(new_data_type))

        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def create_or_replace_business_key_column(
    dbutils: object,
    df: DataFrame,
    business_key_column_name: str,
    key_columns: List[str],
    separator: str = "__",
    check_null_values: bool = True,
) -> DataFrame:
    """Create a standard business key concatenating multiple columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
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

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def create_or_replace_audit_columns(dbutils: object, df: DataFrame) -> DataFrame:
    """Create or replace BrewDat audit columns in the given DataFrame.

    The following audit columns are created/replaced:
        - __insert_gmt_ts: timestamp of when the record was inserted.
        - __update_gmt_ts: timestamp of when the record was last updated.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
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

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def deduplicate_records(
    dbutils: object,
    df: DataFrame,
    key_columns: List[str] = None,
    watermark_column: str = None,
) -> DataFrame:
    """Deduplicate rows from a DataFrame using optional key and watermark columns.

    Do not use orderBy followed by dropDuplicates because it
    requires a coalesce(1) to preserve the order of the rows.
    For more information: https://stackoverflow.com/a/54738843

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        The PySpark DataFrame to modify.
    key_columns : List[str], default=None
        The names of the columns used to uniquely identify each record the table.
    watermark_column : str, default=None
        The name of a datetime column used to select the newest records.

    Returns
    -------
    DataFrame
        The deduplicated PySpark DataFrame.

    See Also
    --------
    pyspark.sql.DataFrame.distinct : Equivalent to SQL's SELECT DISTINCT.
    pyspark.sql.DataFrame.dropDuplicates : Distinct with optional subset parameter.
    """
    try:
        if key_columns is None:
            return df.dropDuplicates()

        if watermark_column is None:
            return df.dropDuplicates(key_columns)

        if not key_columns:
            raise ValueError("No key column was given. If this is intentional, use None instead.")

        return (
            df
            .withColumn("__dedup_row_number", F.row_number().over(
                Window.partitionBy(*key_columns).orderBy(F.col(watermark_column).desc())
            ))
            .filter("__dedup_row_number = 1")
            .drop("__dedup_row_number")
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def cast_all_columns_to_string(
    dbutils: object,
    df: DataFrame,
) -> DataFrame:
    """Recursively cast all DataFrame columns to string type, while
    preserving the nested structure of array, map, and struct columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        The PySpark DataFrame to cast.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with all columns cast to string.
    """
    try:
        expressions = []
        for column in df.schema:
            if column.dataType.typeName() == "string":
                expressions.append(f"`{column.name}`")
            else:
                new_type = _spark_type_to_string_recurse(column.dataType)
                expressions.append(f"CAST(`{column.name}` AS {new_type}) AS `{column.name}`")

        return df.selectExpr(*expressions)

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def flatten_dataframe(
    dbutils: object,
    df: DataFrame,
    except_for: List[str] = [],
    explode_arrays: bool = True,
    recursive: bool = True,
    column_name_separator: str = "__",
) -> DataFrame:
    """Flatten all struct/map columns from a PySpark DataFrame, optionally exploding array columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        The PySpark DataFrame to flatten.
    except_for : List[str], default=[]
        List of columns to be ignored by flattening process.
    explode_arrays : bool, default=True
        When true, all array columns will be exploded.
        Be careful when processing DataFrames with multiple array columns as it may result in Out-of-Memory (OOM) error.
    recursive : bool, default=True
        When true, struct/map/array columns nested inside other struct/map/array columns will also be flattened.
        Otherwise, only top-level complex columns will be flattened and inner columns will keep their original types.
    column_name_separator: str, default="__"
        A string for separating parent and nested column names in the new flattened columns.

    Returns
    -------
    DataFrame
        The flattened PySpark DataFrame.
    """
    try:
        while True:
            # Process struct and map columns
            # And optionally array columns, too
            should_process = any(
                col.name not in except_for
                and (
                    col.dataType.typeName() in ["struct", "map"]
                    or explode_arrays and col.dataType.typeName() == "array"
                )
                for col in df.schema
            )

            if not should_process:
                break

            # Flatten complex data types
            expressions = []
            for column in df.schema:
                if column.name in except_for:
                    expressions.append(column.name)
                elif column.dataType.typeName() == "struct":
                    nested_cols = [F.col(f"`{column.name}`.`{nc}`").alias(f"{column.name}{column_name_separator}{nc}")
                                   for nc in df.select(f"`{column.name}`.*").columns]
                    expressions.extend(nested_cols)
                elif column.dataType.typeName() == "map":
                    map_keys = (
                        df
                        .select(F.explode(F.map_keys(column.name)).alias("__map_key"))
                        .select(F.collect_set("__map_key"))
                        .first()[0]
                    )
                    nested_cols = [F.col(f"`{column.name}`.`{nc}`").alias(f"{column.name}{column_name_separator}{nc}")
                                   for nc in map_keys]
                    expressions.extend(nested_cols)
                elif column.dataType.typeName() == "array" and explode_arrays:
                    df = df.withColumn(column.name, F.explode_outer(column.name))
                    expressions.append(column.name)
                else:
                    expressions.append(column.name)
            df = df.select(expressions)

            if not recursive:
                break

        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def apply_column_mappings(
    dbutils: object,
    df: DataFrame,
    mappings: List[ColumnMapping],
    unmapped_behavior: UnmappedColumnBehavior = UnmappedColumnBehavior.IGNORE_UNMAPPED_COLUMNS,
) -> Tuple[DataFrame, List[str]]:
    """Cast and rename DataFrame columns according to a list of column mappings.

    Optionally raise an exception if the mapping is missing any source column, except
    for metadata columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        The PySpark DataFrame to cast.
    mappings: List[ColumnMapping]
        List of column mapping objects.
    unmapped_behavior: UnmappedColumnBehavior, default=IGNORE_UNMAPPED_COLUMNS
        Specifies the way in which unmapped DataFrame columns should be handled.

    Returns
    -------
    (DataFrame, List[str])
        DataFrame
            The modified PySpark DataFrame with columns properly cast and renamed.
        List[str]
            The list of unmapped DataFrame columns.
    """
    try:
        # Use case insensitive comparison like Spark does
        all_columns = common_utils.list_non_metadata_columns(df)
        mapped_columns = [m.source_column_name.lower() for m in mappings]
        unmapped_columns = [col for col in all_columns if col.lower() not in mapped_columns]
        if unmapped_columns and unmapped_behavior == UnmappedColumnBehavior.FAIL_ON_UNMAPPED_COLUMNS:
            formatted_columns = ", ".join([f"`{col}`" for col in unmapped_columns])
            raise ValueError(f"Columns {formatted_columns} are missing from schema mappings")

        expressions = []
        for mapping in mappings:
            source_expression = mapping.sql_expression or f"`{mapping.source_column_name}`"
            expression = f"CAST({source_expression} AS {mapping.target_data_type}) AS `{mapping.target_column_name}`"
            expressions.append(expression)
        df = df.selectExpr(*expressions)

        return df, unmapped_columns

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def _clean_column_name(column_name: str) -> str:
    """Returns column name formatted into proper pattern.

    Replaces non-alphanumeric characters with underscore and
    strips leading/trailing underscores, except in metadata columns.

    Parameters
    ----------
    column_name : str
        Column name to be formatted.

    Returns
    -------
    str
        Formatted column name.
    """
    # Replace anything that is not alphanumeric or underscore with underscore
    new_column_name = re.sub(r"[^A-Za-z0-9_]+", "_", column_name)

    # Remove leading/trailing underscores
    # Except for leading double underscores in metadata columns
    if new_column_name.startswith("_") and not new_column_name.startswith("__"):
        new_column_name = new_column_name.strip("_")
    else:
        new_column_name = new_column_name.rstrip("_")

    return new_column_name


def _spark_type_clean_field_names_recurse(spark_type: DataType) -> str:
    """Returns a DDL representation of a Spark data type for casting purposes.

    All field names from complex types are replaced with clean field names.

    Parameters
    ----------
    spark_type : DataType
        DataType object that should have its field names cleaned.

    Returns
    -------
    str
        DDL representation of the new Datatype.
    """
    if spark_type.typeName() == "array":
        new_element_type = _spark_type_clean_field_names_recurse(spark_type.elementType)
        return f"array<{new_element_type}>"
    if spark_type.typeName() == "map":
        new_key_type = _spark_type_clean_field_names_recurse(spark_type.keyType)
        new_value_type = _spark_type_clean_field_names_recurse(spark_type.valueType)
        return f"map<{new_key_type},{new_value_type}>"
    if spark_type.typeName() == "struct":
        new_field_types = []
        for name in spark_type.fieldNames():
            new_field_type = _spark_type_clean_field_names_recurse(spark_type[name].dataType)
            new_name = _clean_column_name(name)
            new_field_types.append(f"`{new_name}`: {new_field_type}")
        return "struct<" + ", ".join(new_field_types) + ">"
    return spark_type.typeName()


def _spark_type_to_string_recurse(spark_type: DataType) -> str:
    """Returns a DDL representation of a Spark data type for casting purposes.

    All primitive types (int, bool, etc.) are replaced with the string type.
    Structs, arrays, and maps keep their original structure; however, their
    nested primitive types are replaced by string.

    Parameters
    ----------
    spark_type : DataType
        DataType object to be recursively cast to string.

    Returns
    -------
    str
        DDL representation of the new Datatype.
    """
    if spark_type.typeName() == "array":
        new_element_type = _spark_type_to_string_recurse(spark_type.elementType)
        return f"array<{new_element_type}>"
    if spark_type.typeName() == "map":
        new_key_type = _spark_type_to_string_recurse(spark_type.keyType)
        new_value_type = _spark_type_to_string_recurse(spark_type.valueType)
        return f"map<{new_key_type}, {new_value_type}>"
    if spark_type.typeName() == "struct":
        new_field_types = []
        for name in spark_type.fieldNames():
            new_field_type = _spark_type_to_string_recurse(spark_type[name].dataType)
            new_field_types.append(f"`{name}`: {new_field_type}")
        return "struct<" + ", ".join(new_field_types) + ">"
    return "string"
