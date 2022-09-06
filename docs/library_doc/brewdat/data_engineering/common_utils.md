# common_utils module


### _class_ brewdat.data_engineering.common_utils.ColumnMapping(source_column_name: str, target_data_type: str, sql_expression: Optional[str] = None, target_column_name: Optional[str] = None, nullable: bool = True)
Object the holds the source-to-target-mapping information
for a single column in a DataFrame.


#### source_column_name()
Column name in the source DataFrame.


* **Type**

    str



#### target_data_type()
The data type to which input column will be cast to.


* **Type**

    str



#### sql_expression()
Spark SQL expression to create the target column.
If None, simply cast and possibly rename the source column.


* **Type**

    str, default=None



#### target_column_name()
Column name in the target DataFrame.
If None, use source_column_name as target_column_name.


* **Type**

    str, default=None



#### nullable()
Whether the target column should allow null values.
Used for data quality checks.


* **Type**

    bool, default=True



### _class_ brewdat.data_engineering.common_utils.ReturnObject(status: brewdat.data_engineering.common_utils.RunStatus, target_object: str, num_records_read: int = 0, num_records_loaded: int = 0, num_records_errored_out: int = 0, error_message: str = '', error_details: str = '', old_version_number: Optional[int] = None, new_version_number: Optional[int] = None, effective_data_interval_start: str = '', effective_data_interval_end: str = '')
Object that holds metadata from a data write operation.


#### status()
Resulting status for this write operation.


* **Type**

    RunStatus



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



#### num_records_errored_out()
Number of records that have been rejected.


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



#### old_version_number()
Version number of target object before write operation.


* **Type**

    int, default=None



#### new_version_number()
Version number of target object after write operation.


* **Type**

    int, default=None



#### effective_data_interval_start()
The effective watermark lower bound of the input DataFrame.


* **Type**

    str, default=””



#### effective_data_interval_end()
The effective watermark upper bound of the input DataFrame.


* **Type**

    str, default=””



### _class_ brewdat.data_engineering.common_utils.RunStatus(value)
Available run statuses.


#### FAILED(_ = 'FAILED_ )
Represents a failed run status.


#### SUCCEEDED(_ = 'SUCCEEDED_ )
Represents a succeeded run status.


### brewdat.data_engineering.common_utils.configure_spn_access_for_adls(spark: pyspark.sql.session.SparkSession, dbutils: object, storage_account_names: List[str], key_vault_name: str, spn_client_id: str, spn_secret_name: str, spn_tenant_id: str = 'cef04b19-7776-4a94-b89b-375c77a8f936')
Set up access to an ADLS Storage Account using a Service Principal.

We try to use Spark Context to make it available to the RDD API.
This is a requirement for using spark-xml and similar libraries.
If Spark Context fails, we use Spark Session configuration instead.


* **Parameters**

    
    * **spark** (*SparkSession*) – A Spark session.


    * **dbutils** (*object*) – A Databricks utils object.


    * **storage_account_names** (*List**[**str**]*) – Name of the ADLS Storage Accounts to configure with this SPN.


    * **key_vault_name** (*str*) – Databricks secret scope name. Usually the same as the name of the Azure Key Vault.


    * **spn_client_id** (*str*) – Application (Client) Id for the Service Principal in Azure Active Directory.


    * **spn_secret_name** (*str*) – Name of the secret containing the Service Principal’s client secret.


    * **spn_tenant_id** (*str**, **default="cef04b19-7776-4a94-b89b-375c77a8f936"*) – Tenant Id for the Service Principal in Azure Active Directory.



### brewdat.data_engineering.common_utils.exit_with_last_exception(dbutils: object)
Handle the last unhandled exception, returning an object to the notebook’s caller.

The most recent exception is obtained from sys.exc_info().


* **Parameters**

    **dbutils** (*object*) – A Databricks utils object.



### brewdat.data_engineering.common_utils.exit_with_object(dbutils: object, results: brewdat.data_engineering.common_utils.ReturnObject)
Finish execution returning an object to the notebook’s caller.

Used to return the results of a write operation to the orchestrator.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **results** (*ReturnObject*) – Object containing the results of a write operation.



### brewdat.data_engineering.common_utils.list_non_metadata_columns(df: pyspark.sql.dataframe.DataFrame)
Obtain a list of DataFrame columns except for metadata columns.

Metadata columns are all columns whose name begins with “__”.


* **Parameters**

    **df** (*DataFrame*) – The PySpark DataFrame to inspect.



* **Returns**

    The list of DataFrame columns, except for metadata columns.



* **Return type**

    List[str]
