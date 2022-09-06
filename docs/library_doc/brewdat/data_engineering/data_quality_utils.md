# data_quality_utils module


### _class_ brewdat.data_engineering.data_quality_utils.DataQualityChecker(dbutils: object, df: pyspark.sql.dataframe.DataFrame)
Helper class that provides data quality checks for
a given DataFrame.


#### dbutils()
A Databricks utils object.


* **Type**

    object



#### df()
PySpark DataFrame to validate.


* **Type**

    DataFrame



#### build_df()
Obtain the resulting DataFrame with data quality checks applied.


* **Returns**

    The modified PySpark DataFrame with updated validation results.



* **Return type**

    DataFrame



#### check_column_does_not_match_regular_expression(column_name: str, regular_expression: str, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value does not match the given regular expression.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **regular_expression** (*str*) – Regular expression that column values should NOT match.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_is_alphanumeric(column_name: str, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is alphanumeric, that is, it matches
the regular expression ‘^[A-Za-z0-9]\*$’.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_is_not_null(column_name: str, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is not null.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_is_numeric(column_name: str, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is numeric, that is, it matches
the regular expression ‘^[0-9]\*.?[0-9]\*([Ee][+-]?[0-9]+)?$’.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_length_between(column_name: str, minimum_length: int, maximum_length: int, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s length is within a given range.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **minimum_length** (*int*) – Minimum length for column values.


    * **maximum_length** (*int*) – Maximum length for column values.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_matches_regular_expression(column_name: str, regular_expression: str, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value matches the given regular expression.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **regular_expression** (*str*) – Regular expression that column values should match.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_max_length(column_name: str, maximum_length: int, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s length does not exceed a maximum length.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **maximum_length** (*int*) – Maximum length for column values.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_max_value(column_name: str, maximum_value: Any, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is does not exceed a maximum value.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **maximum_value** (*Any*) – Maximum value for the column.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_min_length(column_name: str, minimum_length: int, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s length is greater than
or equal to a minimum length.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **minimum_length** (*int*) – Minimum length for column values.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_min_value(column_name: str, minimum_value: Any, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is greater than
or equal to a minimum value.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **minimum_value** (*Any*) – Minimum value for the column.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_type_cast(column_name: str, data_type: str, date_format: Optional[str] = None, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate whether a column’s value can be safely cast
to the given data type without generating a null value.

If the check fails, append a failure message to __data_quality_issues.

Optionally, cast to date/timestamp types using a custom date format.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.

Run this quality check on the source DataFrame BEFORE casting.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **data_type** (*str*) – Spark data type used in cast function.


    * **date_format** (*str**, **default=None*) – Optional format string used with to_date() and to_timestamp()
    functions when data_type is date or timestamp, respectively.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_value_between(column_name: str, minimum_value: Any, maximum_value: Any, filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is within a given range.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **minimum_value** (*Any*) – Minimum value for the column.


    * **maximum_value** (*Any*) – Maximum value for the column.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_value_is_in(column_name: str, valid_values: List[Any], filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is in a list of valid values.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **valid_values** (*List**[**Any**]*) – List of valid values.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_column_value_is_not_in(column_name: str, invalid_values: List[Any], filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a column’s value is not in a list of invalid values.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **column_name** (*str*) – Name of the column to be validated.


    * **invalid_values** (*List**[**Any**]*) – List of invalid values.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### _classmethod_ check_columns_exist(dbutils: object, df: pyspark.sql.dataframe.DataFrame, column_names: List[str], raise_exception: bool = True)
Validate that a column exists in the given DataFrame.

Optionally raise an exception in case of missing columns.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – PySpark DataFrame to validate.


    * **column_names** (*List**[**str**]*) – List of columns that should be present in the DataFrame.


    * **raise_exception** (*boolean**, **default=True*) – Whether to raise an exception if any column is missing.



* **Returns**

    The list of missing columns.



* **Return type**

    List[str]



#### check_composite_column_value_is_unique(column_names: List[str], filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Validate that a set of columns has unique values across the entire DataFrame.

If the check fails, append a failure message to __data_quality_issues.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.

This can be used to assert the uniqueness of primary keys, composite or not.


* **Parameters**

    
    * **column_names** (*List**[**str**]*) – List of columns whose composite values should be unique in the DataFrame.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker



#### check_narrow_condition(expected_condition: Union[str, pyspark.sql.column.Column], failure_message: Union[str, pyspark.sql.column.Column], filter_condition: Optional[Union[str, pyspark.sql.column.Column]] = None)
Run a data quality check against every row in the DataFrame.

If the expected condition is False, append a failure message to
the __data_quality_issues metadata column.

Optionally, apply this check only to a subset of rows that match
a custom filter condition.


* **Parameters**

    
    * **expected_condition** (*Union**[**str**, **Column**]*) – PySpark Column expression to evaluate. If this expression
    evaluates to False, the record is considered a bad record.


    * **failure_message** (*Union**[**str**, **Column**]*) – String or PySpark Column expression that generates a message which
    is appended to validation results when expected condition is False.


    * **filter_condition** (*Union**[**str**, **Column**]**, **default=None*) – PySpark Column expression for filtering the rows that this check
    applies to. If this expression evaluates to False, the record
    is not checked.



* **Returns**

    The modified DataQualityChecker instance.



* **Return type**

    DataQualityChecker
