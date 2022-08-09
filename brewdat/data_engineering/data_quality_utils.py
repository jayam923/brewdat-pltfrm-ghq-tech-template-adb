from typing import Any, List, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window

from . import common_utils


def check_narrow_condition(
    dbutils: object,
    df: DataFrame,
    expected_condition: Union[str, Column],
    failure_message: Union[str, Column],
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Run a data quality check against every row in a DataFrame.

    If the expected condition is False, append a failure message to
    the __data_quality_issues metadata column.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    expected_condition : Union[str, Column]
        PySpark Column expression to evaluate. If this expression
        evaluates to False, the record is considered a bad record.
    failure_message : Union[str, Column]
        String or PySpark Column expression that generates a message which
        is appended to validation results when expected condition is False.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if isinstance(failure_message, str):
            failure_message = F.lit(failure_message)

        if isinstance(expected_condition, str):
            expected_condition = F.expr(expected_condition)

        if filter_condition is None:
            filter_condition = F.expr("1 = 1")
        elif isinstance(filter_condition, str):
            filter_condition = F.expr(filter_condition)

        if "__data_quality_issues" not in df.columns:
            df = df.withColumn("__data_quality_issues", F.lit(None).cast("array<string>"))

        return (
            df
            .withColumn(
                "__data_quality_issues",
                F.when(~filter_condition, F.col("__data_quality_issues")),
                F.when(
                    ~expected_condition,
                    F.concat(F.coalesce("__data_quality_issues", F.array()), F.array(failure_message))
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
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate whether a column's value can be safely cast
    to the given data type without generating a null value.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        df = df.withColumn("__value_after_cast", F.col(column_name).cast(data_type))

        expected_condition = F.col("__value_after_cast").isNotNull() | F.col(column_name).isNull()
        failure_message = F.concat(
            F.lit(f"CHECK_TYPE_CAST: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which cannot be safely cast to type {data_type}")
        )
        df = check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

        df = df.drop("__value_after_cast")
        return df

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_is_not_null(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is not null.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).isNotNull()
        failure_message = f"CHECK_NOT_NULL: Column `{column_name}` is null"
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_max_length(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    maximum_length: int,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's length does not exceed a maximum length.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

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
            F.lit(f"CHECK_MAX_LENGTH: Column `{column_name}` has length "),
            F.length(column_name),
            F.lit(f", which is greater than {maximum_length}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_min_length(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_length: int,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's length is greater than
    or equal to a minimum length.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

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
            F.lit(f"CHECK_MIN_LENGTH: Column `{column_name}` has length "),
            F.length(column_name),
            F.lit(f", which is less than {minimum_length}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_length_between(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_length: int,
    maximum_length: int,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's length is within a given range.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if minimum_length <= 0:
            raise ValueError("Minimum length must be greater than 0.")

        if maximum_length <= 0:
            raise ValueError("Maximum length must be greater than 0.")

        if minimum_length > maximum_length:
            raise ValueError("Minimum length must be less than or equal to maximum length.")

        expected_condition = F.length(column_name).between(minimum_length, maximum_length)
        failure_message = F.concat(
            F.lit(f"CHECK_LENGTH_RANGE: Column `{column_name}` has length "),
            F.length(column_name),
            F.lit(f", which is not between {minimum_length} and {maximum_length}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_max_value(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    maximum_value: Any,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is does not exceed a maximum value.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    maximum_value : Any
        Maximum value for the column.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name) <= F.lit(maximum_value)
        failure_message = F.concat(
            F.lit(f"CHECK_MAX_VALUE: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which is greater than {maximum_value}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_min_value(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_value: Any,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is greater than
    or equal to a minimum value.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name) >= F.lit(minimum_value)
        failure_message = F.concat(
            F.lit(f"CHECK_MIN_VALUE: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which is less than {minimum_value}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_between(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    minimum_value: Any,
    maximum_value: Any,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is within a given range.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).between(minimum_value, maximum_value)
        failure_message = F.concat(
            F.lit(f"CHECK_VALUE_RANGE: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which is not between {minimum_value} and {maximum_value}")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_is_in(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    valid_values: List[Any],
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is in a list of valid values.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).isin(valid_values)
        failure_message = F.concat(
            F.lit(f"CHECK_VALUE_IN: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(", which is not in the list of valid values")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_value_is_not_in(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    invalid_values: List[Any],
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is not in a list of invalid values.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = ~F.col(column_name).isin(invalid_values)
        failure_message = F.concat(
            F.lit(f"CHECK_VALUE_NOT_IN: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(", which is in the list of invalid values")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_matches_regular_expression(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    regular_expression: str,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value matches the given regular expression.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

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
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).rlike(regular_expression)
        failure_message = F.concat(
            F.lit(f"CHECK_REGEX_MATCH: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which does not match the regular expression '{regular_expression}'")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_does_not_match_regular_expression(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    regular_expression: str,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value does not match the given regular expression.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    regular_expression : str
        Regular expression that column values should NOT match.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = ~F.col(column_name).rlike(regular_expression)
        failure_message = F.concat(
            F.lit(f"CHECK_REGEX_NOT_MATCH: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which matches the regular expression '{regular_expression}'")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_is_numeric(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is numeric, that is, it matches
    the regular expression '^[0-9]*\.?[0-9]*([Ee][+-]?[0-9]+)?$'.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).rlike("^[0-9]*\.?[0-9]*([Ee][+-]?[0-9]+)?$")
        failure_message = F.concat(
            F.lit(f"CHECK_NUMERIC: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which is not a numeric value")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_column_is_alphanumeric(
    dbutils: object,
    df: DataFrame,
    column_name: str,
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a column's value is alphanumeric, that is, it matches
    the regular expression '^[A-Za-z0-9]*$'.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_name : str
        Name of the column to be validated.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        expected_condition = F.col(column_name).rlike("^[A-Za-z0-9]*$")
        failure_message = F.concat(
            F.lit(f"CHECK_ALPHANUMERIC: Column `{column_name}` has value "),
            F.col(column_name),
            F.lit(f", which is not an alphanumeric value")
        )
        return check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
        )

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def check_composite_column_value_is_unique(
    dbutils: object,
    df: DataFrame,
    column_names: List[str],
    filter_condition: Union[str, Column] = None,
) -> DataFrame:
    """Validate that a set of columns has unique values across the entire DataFrame.

    If the check fails, append a failure message to __data_quality_issues.

    Optionally, apply this check only to a subset of rows that match
    a custom filter condition.

    This can be used to assert the uniqueness of primary keys, composite or not.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_names : List[str]
        List of columns whose composite values should be unique in the DataFrame.
    filter_condition : Union[str, Column], default=None
        PySpark Column expression for filtering the rows that this check
        applies to. If this expression evaluates to False, the record
        is not checked.

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with updated validation results.
    """
    try:
        if not column_names:
            raise ValueError("No column was given")

        df = df.withColumn("__duplicate_count", F.count("*").over(
            Window.partitionBy(*column_names)
        ))

        expected_condition = F.col("__duplicate_count") == 1
        failure_message = F.concat(
            F.lit(f"CHECK_UNIQUE: Column(s) `{column_names}` has value(s) "),
            F.concat_ws(", ", *column_names),
            F.lit(", which is a duplicate value")
        )
        df = check_narrow_condition(
            dbutils=dbutils,
            df=df,
            expected_condition=expected_condition,
            failure_message=failure_message,
            filter_condition=filter_condition,
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
    """Validate that a column exists in the given DataFrame.

    Optionally raise an exception in case of missing columns.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        PySpark DataFrame to validate.
    column_names : List[str]
        List of columns that should be present in the DataFrame.
    raise_exception : boolean, default=True
        Whether to raise an exception if any column is missing.

    Returns
    -------
    List[str]
        The list of missing columns.
    """
    try:
        missing_columns = [col for col in column_names if col not in df.columns]

        if len(missing_columns) > 0 and raise_exception:
            raise KeyError(f"DataFrame is missing required column(s): {', '.join(missing_columns)}")

        return missing_columns

    except Exception:
        common_utils.exit_with_last_exception(dbutils)
