# common_utils module


### _class_ brewdat.data_engineering.common_utils.ReturnObject(status: brewdat.data_engineering.common_utils.RunStatus, target_object: str, num_records_read: int = 0, num_records_loaded: int = 0, error_message: str = '', error_details: str = '')
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



#### error_message()
Error message describing whichever error that occurred.


* **Type**

    str, default=””



#### error_details()
Detailed error message or stack trace for the above error.


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

We use Hadoop Configuration to make it available to the RDD API.
This is a requirement for using spark-xml and similar libraries.


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
