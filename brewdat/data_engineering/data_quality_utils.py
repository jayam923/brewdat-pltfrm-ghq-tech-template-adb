from typing import Any, List, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window

from . import common_utils


def check_narrow_condition(
    dbutils: object,
    df: DataFrame,
    expected_condition: Column,
    failure_message: Union[str, Column],
) -> DataFrame:
    """Run a data quality check against every row in a DataFrame.

    If the given condition is False, append a failure message to
    the __data_quality_issues metadata column.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    expected_condition : Column
        PySpark Column expression to evaluate. If this expression
        evaluates to False, the record is considered a bad record.
    failure_message : Union[str, Column]
        String or PySpark Column expression that generates a message which
        is appended to validation results when expected condition is False.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if type(failure_message) == "str":
            failure_message = F.lit(failure_message)

        if "__data_quality_issues" not in df.columns:
            df = df.withColumn("__data_quality_issues", F.lit(None).cast("string"))

        return (
            df
            .withColumn(
                "__data_quality_issues",
                F.when(
                    ~expected_condition,
                    F.concat_ws("; ", "__data_quality_issues", failure_message)
                )
                .otherwise(F.col("__data_quality_issues"))
            )
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_type_cast(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    data_type: str,
) -> DataFrame:
    """Validate whether a column's value can be safely cast
    to the given data type without generating a null value.

    Run this quality check on the source DataFrame BEFORE casting.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    data_type : str
        Spark data type used in cast function.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        df = df.withColumn("__value_after_cast", F.col(column_name).cast(data_type))

        expected_condition = F.col("__value_after_cast").isNotNull() | F.col(column_name).isNull()
        failure_message = F.concat(
            f"Column `{column_name}` has value ",
            F.col(column_name),
            f", which cannot be safely cast to type {data_type}"
        )
        df = check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

        df = df.drop("__value_after_cast")
        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_is_not_null(
    dbutils: object,
    df: DataFrame,
    column_name: str,
) -> DataFrame:
    """Validate that a column's value is not null.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).isNotNull()
        failure_message = f"Column `{column_name}` is null"
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_max_length(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    maximum_length: int,
) -> DataFrame:
    """Validate that a column's length does not exceed a maximum length.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    maximum_length : int
        Maximum length for column values.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if maximum_length <= 0:
            raise ValueError("Maximum length must be greater than 0.")

        expected_condition = F.length(column_name) <= maximum_length
        failure_message = F.concat(
            f"Column `{column_name}` has length ",
            F.length(column_name),
            f", which is greater than {maximum_length}"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_min_length(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_length: int,
) -> DataFrame:
    """Validate that a column's length is greater than
    or equal to a minimum length.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    minimum_length : int
        Minimum length for column values.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if minimum_length <= 0:
            raise ValueError("Minimum length must be greater than 0.")

        expected_condition = F.length(column_name) >= minimum_length
        failure_message = F.concat(
            f"Column `{column_name}` has length ",
            F.length(column_name),
            f", which is less than {minimum_length}"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_length_between(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_length: int,
    maximum_length: int,
) -> DataFrame:
    """Validate that a column's length is within a given range.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    minimum_length : int
        Minimum length for column values.
    maximum_length : int
        Maximum length for column values.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if minimum_length is not None and minimum_length <= 0:
            raise ValueError("Minimum length must be greater than 0.")

        if maximum_length is not None and maximum_length <= 0:
            raise ValueError("Maximum length must be greater than 0.")

        expected_condition = F.length(column_name).between(minimum_length, maximum_length)
        failure_message = F.concat(
            f"Column `{column_name}` has length ",
            F.length(column_name),
            f", which is not between {minimum_length} and {maximum_length}"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_between(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_value: Any,
    maximum_value: Any,
) -> DataFrame:
    """Validate that a column's value is within a given range.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    minimum_value : Any
        Minimum value for the column.
    maximum_value : Any
        Maximum value for the column.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).between(minimum_value, maximum_value)
        failure_message = F.concat(
            f"Column `{column_name}` has value ",
            F.col(column_name),
            f", which is not between {minimum_value} and {maximum_value}"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_is_in(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    valid_values: List[Any],
) -> DataFrame:
    """Validate that a column's value is in a list of valid values.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    valid_values : List[Any]
        List of valid values.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).isin(valid_values)
        failure_message = F.concat(
            f"Column `{column_name}` has value ",
            F.col(column_name),
            ", which is not in the list of valid values"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_is_not_in(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    invalid_values: List[Any],
) -> DataFrame:
    """Validate that a column's value is not in a list of invalid values.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    invalid_values : List[Any]
        List of invalid values.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = ~F.col(column_name).isin(invalid_values)
        failure_message = F.concat(
            f"Column `{column_name}` has value ",
            F.col(column_name),
            ", which is in the list of invalid values"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_matches_regular_expression(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    regular_expression: str,
) -> DataFrame:
    """Validate that a column's value matches the given regular expression.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    regular_expression : str
        Regular expression that column values should match.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).rlike(regular_expression)
        failure_message = F.concat(
            f"Column `{column_name}` has value ",
            F.col(column_name),
            f", which does not match the regular expression '{regular_expression}'"
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_composite_columns_value_is_unique(
    dbutils: object,
    df: DataFrame,
    column_names: List[str],
) -> DataFrame:
    """Validate that a set of columns has unique values across the entire DataFrame.

    This can be used to assert the uniqueness of primary keys, composite or not.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_names : List[str]
        List of columns whose composite values should be unique in the DataFrame.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        df = df.withColumn("__duplicate_count", F.count("*").over(
            Window.partitionBy(*column_names)
        ))

        expected_condition = F.col("__duplicate_count") == 1
        failure_message = F.concat(
            f"Column(s) `{column_names}` has value(s) ",
            F.concat_ws(", ", *column_names),
            ", which is a duplicate value"
        )
        df = check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
        )

        df = df.drop("__duplicate_indicator")
        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_columns_exist(
    dbutils: object,
    df: DataFrame,
    column_names: List[str],
    raise_exception: bool = True,
) -> List[str]:
    """Checks the field column values against valid values

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_names : List[str]
        List of columns that should be present in the DataFrame.
    raise_error : boolean, default=True
        Whether to raise an exception if any column is missing.

    Returns
    -------
    List[str]
        The list of missing columns.
    """
    try:
        missing_columns = [col for col in column_names if col not in df.columns]

        if len(missing_columns) > 0 and raise_exception:
            raise KeyError(f"DataFrame is missing required column(s): {missing_columns}")

        return missing_columns

    except Exception:
        common_utils.exit_with_last_exception(dbutils)
