# write_utils module


### _class_ brewdat.data_engineering.write_utils.BadRecordHandlingMode(value)
Specifies the way in which bad records should be handled.

Bad records are rows where __data_quality_issues column exists and is not null.


#### REJECT(_ = 'REJECT_ )
Filter out bad records and append them to a separate error table.


#### WARN(_ = 'WARN_ )
Write both bad and good records to target table and warn if bad records are present.


### _class_ brewdat.data_engineering.write_utils.LoadType(value)
Specifies the way in which the table should be loaded.


#### APPEND_ALL(_ = 'APPEND_ALL_ )
Load type where all records in the DataFrame are written into an table.
*Attention*: use this load type only for Bronze tables, as it is bad for backfilling.


#### APPEND_NEW(_ = 'APPEND_NEW_ )
Load type where only new records in the DataFrame are written into an existing table.
Records for which the key already exists in the table are ignored.


#### OVERWRITE_PARTITION(_ = 'OVERWRITE_PARTITION_ )
Load type for overwriting a single partition based on partitionColumns.
This deletes records that are not present in the DataFrame for the chosen partition.
The df must be filtered such that it contains a single partition.


#### OVERWRITE_TABLE(_ = 'OVERWRITE_TABLE_ )
Load type where the entire table is rewritten in every execution.
Avoid whenever possible, as this is not good for large tables.
This deletes records that are not present in the DataFrame.


#### TYPE_2_SCD(_ = 'TYPE_2_SCD_ )
Load type that implements the standard type-2 Slowly Changing Dimension.
This essentially uses an upsert that keeps track of all previous versions of each record.
For more information: [https://en.wikipedia.org/wiki/Slowly_changing_dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension)


#### UPSERT(_ = 'UPSERT_ )
Load type where records of a df are appended as new records or update existing records based on the key.
This does NOT delete existing records that are not included in the DataFrame.


### _class_ brewdat.data_engineering.write_utils.SchemaEvolutionMode(value)
Specifies the way in which schema mismatches should be handled.


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
For more information: [https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution)
*Attention*: This schema evolution mode is not implemented on this library yet!


### brewdat.data_engineering.write_utils.write_delta_table(spark: pyspark.sql.session.SparkSession, df: pyspark.sql.dataframe.DataFrame, location: str, database_name: str, table_name: str, load_type: brewdat.data_engineering.write_utils.LoadType, key_columns: List[str] = [], partition_columns: List[str] = [], schema_evolution_mode: brewdat.data_engineering.write_utils.SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS, bad_record_handling_mode: brewdat.data_engineering.write_utils.BadRecordHandlingMode = BadRecordHandlingMode.WARN, time_travel_retention_days: int = 30, auto_broadcast_join_threshold: int = 52428800, enable_caching: bool = True)
Write the DataFrame as a delta table.


* **Parameters**

    
    * **spark** (*SparkSession*) – A Spark session.


    * **df** (*DataFrame*) – PySpark DataFrame to modify.


    * **location** (*str*) – Absolute Delta Lake path for the physical location of this delta table.


    * **database_name** (*str*) – Name of the database/schema for the table in the metastore.
    Database is created if it does not exist.


    * **table_name** (*str*) – Name of the table in the metastore.


    * **load_type** (*BrewDatLibrary.LoadType*) – Specifies the way in which the table should be loaded.
    See documentation for BrewDatLibrary.LoadType.


    * **key_columns** (*List**[**str**]**, **default=**[**]*) – The names of the columns used to uniquely identify each record in the table.
    Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.


    * **partition_columns** (*List**[**str**]**, **default=**[**]*) – The names of the columns used to partition the table.


    * **schema_evolution_mode** (*BrewDatLibrary.SchemaEvolutionMode**, **default=ADD_NEW_COLUMNS*) – Specifies the way in which schema mismatches should be handled.
    See documentation for BrewDatLibrary.SchemaEvolutionMode.


    * **bad_record_handling_mode** (*BrewDatLibrary.BadRecordHandlingMode**, **default=WARN*) – Specifies the way in which bad records should be handled.
    See documentation for BrewDatLibrary.BadRecordHandlingMode.


    * **time_travel_retention_days** (*int**, **default=30*) – Number of days for retaining time travel data in the Delta table.
    Used to limit how many old snapshots are preserved during the VACUUM operation.
    For more information: [https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch)


    * **auto_broadcast_join_threshold** (*int**, **default=52428800*) – Configures the maximum size in bytes for a table that will be broadcast to all worker
    nodes when performing a join. Default value in bytes represents 50 MB.


    * **enable_caching** (*bool**, **default=True*) – Cache the DataFrame so that transformations are not recomputed multiple times
    during couting, bad record handling, or writing with TYPE_2_SCD.



* **Returns**

    Object containing the results of a write operation.



* **Return type**

    [ReturnObject](common_utils.md#brewdat.data_engineering.common_utils.ReturnObject)
