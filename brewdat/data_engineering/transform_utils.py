import re
from datetime import datetime
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from pyspark.sql.window import Window

from . import common_utils


def clean_column_names(
    dbutils: object,
    df: DataFrame,
    except_for: List[str] = [],
) -> DataFrame:
    """Normalize the name of all the columns in a given DataFrame.

    Uses BrewDat's standard approach as seen in other Notebooks.
    Improved to also trim (strip) whitespaces.

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
        column_names = df.schema.names
        for column_name in column_names:
            if column_name in except_for:
                continue  # Skip

            # \W is "anything that is not alphanumeric or underscore"
            # Equivalent to [^A-Za-z0-9_]
            new_column_name = re.sub(r"\W+", "_", column_name.strip())
            if column_name != new_column_name:
                df = df.withColumnRenamed(column_name, new_column_name)
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


def flatten_struct_columns(
    dbutils: object,
    df: DataFrame,
    except_for: List[str] = [],
    recursive: bool = False
) -> DataFrame:
    flat_cols = [c.name for c in df.schema
                 if c.dataType.typeName() != 'struct' or c.name in except_for]
    nested_cols = [c.name for c in df.schema
                   if c.dataType.typeName() == 'struct' and c.name not in except_for]
    unnested_cols = [F.col(f'{nc}.{c}').alias(f'{nc}__{c}')
                     for nc in nested_cols
                     for c in df.select(f'{nc}.*').columns]

    flat_df = df.select(flat_cols + unnested_cols)

    remain_nested_cols = [c.name for c in flat_df.schema
                          if c.dataType.typeName() == 'struct' and c.name not in except_for]

    if recursive and remain_nested_cols:
        return flatten_struct_columns(dbutils=dbutils, df=flat_df, except_for=except_for, recursive=recursive)
    else:
        return flat_df


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
