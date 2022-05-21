# Welcome to brewdat-pltfrm-ghq-tech-template-adb’s documentation!


### _class_ brewdat.data_engineering.utils.BrewDatLibrary(spark: pyspark.sql.session.SparkSession, dbutils: object)
Reusable functions for all BrewDat projects.


#### spark()
A Spark session.


* **Type**

    SparkSession



#### dbutils()
A Databricks utils object.


* **Type**

    object



#### _class_ LoadType(value)
An enumeration.


#### APPEND_ALL(_ = 'APPEND_ALL_ )
Load type where all records in the df are written into an table.


#### APPEND_NEW(_ = 'APPEND_NEW_ )
Load type where only new records in the df are written into an existing table.
Records for which the key already exists in the table are ignored.


#### OVERWRITE_PARTITION(_ = 'OVERWRITE_PARTITION_ )
Load type for overwriting a single partition based on partitionColumns.
This deletes records that are not present in df for the chosen partition.
The df must be filtered such that it contains a single partition.


#### OVERWRITE_TABLE(_ = 'OVERWRITE_TABLE_ )
Load type where the entire table is rewritten in every execution.
Avoid whenever possible, as this is not good for large tables.
This deletes records that are not present in df.


#### TYPE_2_SCD(_ = 'TYPE_2_SCD_ )
Load type that implements the standard type-2 Slowly Changing Dimension implementation.
This essentially uses an upsert that keeps track of all previous versions of each record.
For more information: [https://en.wikipedia.org/wiki/Slowly_changing_dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension) .


#### UPSERT(_ = 'UPSERT_ )
Load type where records of a df are appended as new records or update existing records based on the key.
This does NOT delete existing records that are not included in df.


#### _class_ RawFileFormat(value)
An enumeration.


#### CSV(_ = 'CSV_ )
CSV format.


#### DELTA(_ = 'DELTA_ )
Delta Lake format.


#### ORC(_ = 'ORC_ )
ORC format.


#### PARQUET(_ = 'PARQUET_ )
Parquet format.


#### _class_ ReturnObject(\*args, \*\*kwargs)
Object that holds metadata from a data write operation.


#### status()
Resulting status for this write operation.


* **Type**

    str



#### target_object()
Target object that we intended to write to.


* **Type**

    str



#### num_records_read()
Number of records read from the DataFrame.


* **Type**

    int, default=0



#### num_records_loaded()
Number of records written to the target table.


* **Type**

    int, default=0



#### error_message()
Error message describing whichever error that occurred.


* **Type**

    str, default=””



#### error_details()
Detailed error message or stack trace for the above error.


* **Type**

    str, default=””



#### _class_ RunStatus(value)
An enumeration.


#### FAILED(_ = 'FAILED_ )
Represents a failed run status.


#### SUCCEEDED(_ = 'SUCCEEDED_ )
Represents a succeeded run status.


#### _class_ SchemaEvolutionMode(value)
An enumeration.


#### ADD_NEW_COLUMNS(_ = 'ADD_NEW_COLUMNS_ )
Schema evolution through adding new columns to the target table.
This is the same as using the option “mergeSchema”.


#### FAIL_ON_SCHEMA_MISMATCH(_ = 'FAIL_ON_SCHEMA_MISMATCH_ )
Fail if the table’s schema is not compatible with the DataFrame’s.
This is the default Spark behavior when no option is given.


#### IGNORE_NEW_COLUMNS(_ = 'IGNORE_NEW_COLUMNS_ )
Drop DataFrame columns that do not exist in the table’s schema.
Does nothing if the table does not yet exist in the Hive metastore.


#### OVERWRITE_SCHEMA(_ = 'OVERWRITE_SCHEMA_ )
Overwrite the table’s schema with the DataFrame’s schema.
This is the same as using the option “overwriteSchema”.


#### RESCUE_NEW_COLUMNS(_ = 'RESCUE_NEW_COLUMNS_ )
Create a new struct-type column to collect data for new columns.
This is the same strategy used in AutoLoader’s rescue mode.
For more information: [https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution) .


#### clean_column_names(df: pyspark.sql.dataframe.DataFrame, except_for: List[str] = [])
Normalize the name of all the columns in a given DataFrame.

Uses BrewDat’s standard approach as seen in other Notebooks.
Improved to also trim (strip) whitespaces.


* **Parameters**

    
    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **except_for** (*List**[**str**]**, **default=**[**]*) – A list of column names that should NOT be modified.



* **Returns**

    The modified PySpark DataFrame with renamed columns.



* **Return type**

    DataFrame



#### create_or_replace_audit_columns(df: pyspark.sql.dataframe.DataFrame)
Create or replace BrewDat audit columns in the given DataFrame.

The following audit columns are created/replaced:

    _insert_gmt_ts: timestamp of when the record was inserted.
    _update_gmt_ts: timestamp of when the record was last updated.


* **Parameters**

    **df** (*DataFrame*) – The PySpark DataFrame to modify.



* **Returns**

    The modified PySpark DataFrame with audit columns.



* **Return type**

    DataFrame



#### create_or_replace_business_key_column(df: pyspark.sql.dataframe.DataFrame, business_key_column_name: str, key_columns: List[str], separator: str = '__')
Create a standard business key concatenating multiple columns.


* **Parameters**

    
    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **business_key_column_name** (*str*) – The name of the concatenated business key column.


    * **key_columns** (*List**[**str**]*) – The names of the columns used to uniquely identify each record the table.


    * **separator** (*str**, **default="__"*) – A string to separate the values of each column in the business key.



* **Returns**

    The PySpark DataFrame with the desired business key.



* **Return type**

    DataFrame



#### deduplicate_records(df: pyspark.sql.dataframe.DataFrame, key_columns: List[str], watermark_column: str)
Deduplicate rows from a DataFrame using key and watermark columns.

We do not use orderBy followed by dropDuplicates because it
would require a coalesce(1) to preserve the order of the rows.


* **Parameters**

    
    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **key_columns** (*List**[**str**]*) – The names of the columns used to uniquely identify each record the table.


    * **watermark_column** (*str*) – The name of a datetime column used to select the newest records.



* **Returns**

    The deduplicated PySpark DataFrame.



* **Return type**

    DataFrame



#### drop_empty_columns(df: pyspark.sql.dataframe.DataFrame, except_for: List[str] = [])
Drop columns which are null or empty for all the rows in the DataFrame.


* **Parameters**

    
    * **df** (*DataFrame*) – The PySpark DataFrame to modify.


    * **except_for** (*List**[**str**]**, **default=**[**]*) – A list of column names that should NOT be dropped.



* **Returns**

    The modified PySpark DataFrame.



* **Return type**

    DataFrame



#### exit_with_last_exception()
Handle the last unhandled exception, returning an object to the notebook’s caller.

The most recent exception is obtained from sys.exc_info().


#### exit_with_object(results: brewdat.data_engineering.utils.BrewDatLibrary.ReturnObject)
Finish execution returning an object to the notebook’s caller.

Used to return the results of a write operation to the orchestrator.


* **Parameters**

    **results** (*ReturnObject*) – Object containing the results of a write operation.



#### generate_bronze_table_location(target_zone: str, target_business_domain: str, target_system_name: str, table_name: str)
Build the standard location for a Bronze table.


* **Parameters**

    
    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **target_system_name** (*str*) – Name of the source system.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str



#### generate_gold_table_location(target_zone: str, target_business_domain: str, project: str, database_name: str, table_name: str)
Build the standard location for a Gold table.


* **Parameters**

    
    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **project** (*str*) – Project of the target dataset.


    * **database_name** (*str*) – Name of the target database for the table in the metastore.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str



#### generate_silver_table_location(target_zone: str, target_business_domain: str, target_system_name: str, table_name: str)
Build the standard location for a Silver table.


* **Parameters**

    
    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **target_system_name** (*str*) – Name of the source system.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str



#### read_raw_dataframe(file_format: brewdat.data_engineering.utils.BrewDatLibrary.RawFileFormat, location: str)
Read a DataFrame from the Raw Layer. Convert all data types to string.


* **Parameters**

    
    * **file_format** (*RawFileFormat*) – The raw file format use in this dataset (CSV, PARQUET, etc.).


    * **location** (*str*) – Absolute Data Lake path for the physical location of this dataset.



* **Returns**

    The PySpark DataFrame read from the Raw Layer.



* **Return type**

    DataFrame



#### write_delta_table(df: pyspark.sql.dataframe.DataFrame, location: str, schema_name: str, table_name: str, load_type: brewdat.data_engineering.utils.BrewDatLibrary.LoadType, key_columns: List[str] = [], partition_columns: List[str] = [], schema_evolution_mode: brewdat.data_engineering.utils.BrewDatLibrary.SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS)
Write the DataFrame as a delta table.


* **Parameters**

    
    * **df** (*DataFrame*) – PySpark DataFrame to modify.


    * **location** (*str*) – Absolute Delta Lake path for the physical location of this delta table.


    * **schema_name** (*str*) – Name of the schema/database for the table in the metastore.
    Schema is created if it does not exist.


    * **table_name** (*str*) – Name of the table in the metastore.


    * **load_type** (*BrewDatLibrary.LoadType*) – Specifies the way in which the table should be loaded.


    * **key_columns** (*List**[**str**]**, **default=**[**]*) – The names of the columns used to uniquely identify each record the table.
    Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.


    * **partition_columns** (*List**[**str**]**, **default=**[**]*) – The names of the columns used to partition the table.


    * **schema_evolution_mode** (*BrewDatLibrary.SchemaEvolutionMode**, **default=ADD_NEW_COLUMNS*) – Specifies the way in which schema mismatches should be handled.



* **Returns**

    Object containing the results of a write operation.



* **Return type**

    ReturnObject


# Indices and tables


* [Index](genindex.md)


* [Module Index](py-modindex.md)


* [Search Page](search.md)
