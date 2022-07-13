# transform_utils module


### brewdat.data_engineering.transform_utils.cast_all_columns_to_string(dbutils: object, df: pyspark.sql.dataframe.DataFrame)
Recursively cast all DataFrame columns to string type, while
preserving the nested structure of array, map, and struct columns.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to cast.



* **Returns**

    The modified PySpark DataFrame with all columns cast to string.



* **Return type**

    DataFrame



### brewdat.data_engineering.transform_utils.clean_column_names(dbutils: object, df: pyspark.sql.dataframe.DataFrame, except_for: List[str] = [])
Normalize the name of all the columns in a given DataFrame.

Uses BrewDat’s standard approach as seen in other Notebooks.
Improved to also trim (strip) whitespaces.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **except_for** (*List**[**str**]**, **default=**[**]*) – A list of column names that should NOT be modified.



* **Returns**

    The modified PySpark DataFrame with renamed columns.



* **Return type**

    DataFrame



### brewdat.data_engineering.transform_utils.create_or_replace_audit_columns(dbutils: object, df: pyspark.sql.dataframe.DataFrame)
Create or replace BrewDat audit columns in the given DataFrame.

The following audit columns are created/replaced:

    
    * __insert_gmt_ts: timestamp of when the record was inserted.


    * __update_gmt_ts: timestamp of when the record was last updated.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to modify.



* **Returns**

    The modified PySpark DataFrame with audit columns.



* **Return type**

    DataFrame



### brewdat.data_engineering.transform_utils.create_or_replace_business_key_column(dbutils: object, df: pyspark.sql.dataframe.DataFrame, business_key_column_name: str, key_columns: List[str], separator: str = '__', check_null_values: bool = True)
Create a standard business key concatenating multiple columns.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **business_key_column_name** (*str*) – The name of the concatenated business key column.


    * **key_columns** (*List**[**str**]*) – The names of the columns used to uniquely identify each record the table.


    * **separator** (*str**, **default="__"*) – A string to separate the values of each column in the business key.


    * **check_null_values** (*bool**, **default=True*) – Whether to check if the given key columns contain NULL values.
    Throw an error if any NULL value is found.



* **Returns**

    The PySpark DataFrame with the desired business key.



* **Return type**

    DataFrame



### brewdat.data_engineering.transform_utils.deduplicate_records(dbutils: object, df: pyspark.sql.dataframe.DataFrame, key_columns: Optional[List[str]] = None, watermark_column: Optional[str] = None)
Deduplicate rows from a DataFrame using optional key and watermark columns.

Do not use orderBy followed by dropDuplicates because it
requires a coalesce(1) to preserve the order of the rows.
For more information: [https://stackoverflow.com/a/54738843](https://stackoverflow.com/a/54738843)


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **key_columns** (*List**[**str**]**, **default=None*) – The names of the columns used to uniquely identify each record the table.


    * **watermark_column** (*str**, **default=None*) – The name of a datetime column used to select the newest records.



* **Returns**

    The deduplicated PySpark DataFrame.



* **Return type**

    DataFrame



### brewdat.data_engineering.transform_utils.flatten_dataframe(dbutils: object, df: pyspark.sql.dataframe.DataFrame, except_for: List[str] = [], explode_arrays: bool = True, recursive: bool = True, column_name_separator: str = '__')
Flatten all struct/map columns from a PySpark DataFrame, optionally exploding array columns.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **df** (*DataFrame*) – The PySpark DataFrame to flatten.


    * **except_for** (*List**[**str**]**, **default=**[**]*) – List of columns to be ignored by flattening process.


    * **explode_arrays** (*bool**, **default=True*) – When true, all array columns will be exploded.
    Be careful when processing DataFrames with multiple array columns as it may result in Out-of-Memory (OOM) error.


    * **recursive** (*bool**, **default=True*) – When true, struct/map/array columns nested inside other struct/map/array columns will also be flattened.
    Otherwise, only top-level complex columns will be flattened and inner columns will keep their original types.


    * **column_name_separator** (*str**, **default="__"*) – A string for separating parent and nested column names in the new flattened columns.



* **Returns**

    The flattened PySpark DataFrame.



* **Return type**

    DataFrame
