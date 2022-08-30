# lakehouse_utils module


### brewdat.data_engineering.lakehouse_utils.assert_valid_business_domain(business_domain)
Assert that given business domain is valid.

Valid business domains include: compliance, finance, marketing, people, sales, supply, tech.


* **Parameters**

    **business_domain** (*str*) – Business domain of the target dataset.



### brewdat.data_engineering.lakehouse_utils.assert_valid_folder_name(folder_name)
Assert that given folder name is valid.

Folder names must start with an alphanumeric character, and must only
contain alphanumeric characters, dash (-), dot (.), or underscore (_).


* **Parameters**

    **folder_name** (*str*) – Name of a folder in the target dataset’s path.



### brewdat.data_engineering.lakehouse_utils.assert_valid_zone(zone)
Assert that given zone is valid.

Valid zones include: afr, apac, eur, ghq, maz, naz, saz.


* **Parameters**

    **zone** (*str*) – Zone of the target dataset.



### brewdat.data_engineering.lakehouse_utils.generate_bronze_table_location(dbutils: object, lakehouse_bronze_root: str, target_zone: str, target_business_domain: str, source_system: str, table_name: str)
Build the standard location for a Bronze table.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **lakehouse_bronze_root** (*str*) – Root path to the Lakehouse’s Bronze layer.
    Format: “abfss://bronze@storage_account.dfs.core.windows.net”.
    Value varies by environment, so you should use environment variables.


    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **source_system** (*str*) – Name of the source system.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str



### brewdat.data_engineering.lakehouse_utils.generate_gold_table_location(dbutils: object, lakehouse_gold_root: str, target_zone: str, target_business_domain: str, data_product: str, database_name: str, table_name: str)
Build the standard location for a Gold table.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **lakehouse_gold_root** (*str*) – Root path to the Lakehouse’s Gold layer.
    Format: “abfss://gold@storage_account.dfs.core.windows.net”.
    Value varies by environment, so you should use environment variables.


    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **data_product** (*str*) – Data product of the target dataset.


    * **database_name** (*str*) – Name of the target database for the table in the metastore.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str



### brewdat.data_engineering.lakehouse_utils.generate_silver_table_location(dbutils: object, lakehouse_silver_root: str, target_zone: str, target_business_domain: str, source_system: str, table_name: str)
Build the standard location for a Silver table.


* **Parameters**

    
    * **dbutils** (*object*) – A Databricks utils object.


    * **lakehouse_silver_root** (*str*) – Root path to the Lakehouse’s Silver layer.
    Format: “abfss://silver@storage_account.dfs.core.windows.net”.
    Value varies by environment, so you should use environment variables.


    * **target_zone** (*str*) – Zone of the target dataset.


    * **target_business_domain** (*str*) – Business domain of the target dataset.


    * **source_system** (*str*) – Name of the source system.


    * **table_name** (*str*) – Name of the target table in the metastore.



* **Returns**

    Standard location for the delta table.



* **Return type**

    str
