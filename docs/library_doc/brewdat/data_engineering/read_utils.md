# read_utils module


### _class_ brewdat.data_engineering.read_utils.RawFileFormat(value)
Supported raw file formats.


#### AVRO(_ = 'AVRO_ )
Avro format.


#### CSV(_ = 'CSV_ )
Delimited text format.


#### DELTA(_ = 'DELTA_ )
Delta format.


#### EXCEL(_ = 'EXCEL_ )
EXCEL formats.


#### JSON(_ = 'JSON_ )
JSON format.


#### ORC(_ = 'ORC_ )
ORC format.


#### PARQUET(_ = 'PARQUET_ )
Parquet format.


#### XML(_ = 'XML_ )
XML format.


### brewdat.data_engineering.read_utils.read_raw_dataframe(spark: pyspark.sql.session.SparkSession, dbutils: object, file_format: brewdat.data_engineering.read_utils.RawFileFormat, location: str, cast_all_to_string: bool = True, csv_has_headers: bool = True, csv_delimiter: str = ',', csv_escape_character: str = '"', excel_sheet_name: Optional[str] = None, excel_has_headers: bool = True, json_is_multiline: bool = True, xml_row_tag: str = 'row', additional_options: dict = {})
Read a DataFrame from the Raw Layer.


* **Parameters**

    
    * **spark** (*SparkSession*) – A Spark session.


    * **dbutils** (*object*) – A Databricks utils object.


    * **file_format** (*RawFileFormat*) – The raw file format use in this dataset (CSV, PARQUET, etc.).


    * **location** (*str*) – Absolute Data Lake path for the physical location of this dataset.
    Format: “abfss://container@storage_account.dfs.core.windows.net/path/to/dataset/”.


    * **cast_all_to_string** (*bool**, **default=True*) – Whether to cast all non-string values to string.
    Useful to maximize schema compatibility in the Bronze layer.


    * **csv_has_headers** (*bool**, **default=True*) – Whether the CSV file has a header row.


    * **csv_delimiter** (*str**, **default="**,**"*) – Delimiter string for CSV file format.


    * **csv_escape_character** (*str**, **default="""*) – Escape character for CSV file format.


    * **excel_sheet_name** (*str*) – Sheet name for EXCEL file format.
    Use None to get all sheets.


    * **excel_has_headers** (*bool**, **default=True*) – Whether the Excel file has a header row.


    * **json_is_multiline** (*bool**, **default=True*) – Set to True when JSON file has a single record spanning several lines.
    Set to False when JSON file has one record per line (JSON Lines format).


    * **xml_row_tag** (*str**, **default="row"*) – Name of the XML tag to treat as DataFrame rows.


    * **additional_options** (*dict**, **default={}*) – Dictionary with additional options for spark.read.



* **Returns**

    The PySpark DataFrame read from the Raw Layer.



* **Return type**

    DataFrame
