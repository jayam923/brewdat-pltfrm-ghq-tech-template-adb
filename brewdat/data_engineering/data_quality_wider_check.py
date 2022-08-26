import datetime
import math
import great_expectations as ge
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,FloatType
from delta.tables import DeltaTable
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core import ExpectationValidationResult
from great_expectations.validator.validator import Validator
from .import common_utils


class DataQualityCheck():
    """Helper class that provides data quality checks for given DataFrame.
    
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to write.
        
    Returns
    -------
    SparkDFDataset
        SparkDFDataset object for great expectation
    """
    def __init__(self, df: DataFrame,dbutils: object,spark: SparkSession):
        self.validator = ge.dataset.SparkDFDataset(df)
        self.result_list = []
        self.dbutils=dbutils
        self.spark=spark
        
    def get_wider_dq_results( self,
        spark: SparkSession, 
        dbutils: object, 
        with_history = None) -> DataFrame:
        """Create fuction to return dq check results in dataframe

        Parameters
        ----------
        spark : SparkSession
            A Spark session.
        dbutils : object
            A Databricks utils object.


        Returns
        -------
        DataFrame
            Dataframe for history and latest records which are loaded
        """
        try:
            result_schema = (
                 StructType(fields=[StructField('function_name',StringType()),
                                                   StructField('result_value',StringType()), 
                                                   StructField('expected_percentage',FloatType()),
                                                   StructField('range',StringType()),
                                                   StructField('status',StringType()),
                                                   StructField('comments',StringType())])
                 )                   
            result_df= spark.createDataFrame([*set(self.result_list)], result_schema)
            return result_df
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)   
    
    
    def __get_delta_tables_history_dataframe(self,
        target_location: str, 
        older_version : int,
        latest_version : int )-> DataFrame:
        """ Create function to get the hitory and latest version of given table location
        Parameters
        ----------
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        result : object
            A DeltaTable object.
        latet_version:int
            deltalake latest version number
        older_version : int
            deltalake older version number
        Returns
        -------
        DataFrame
            Dataframe for history and latest records which are loaded in delta table
        """
        try:

            latest_df = self.spark.read.format("delta").option("versionAsOf", latest_version).load(target_location)
            history_df = self.spark.read.format("delta").option("versionAsOf", older_version).load(target_location)
            return latest_df, history_df

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)




    def __get_result_list(self,
        result : ExpectationValidationResult,
        resultlist =None,
        dq_result =None,
        result_value = None,
        dq_unexpected_records = None,
        dq_unexpected_percent = None,
        dq_function_name = None,
        dq_min_value = None,
        dq_max_value = None,
        dq_column_name = None,
        dq_mostly= None,
        dq_range = None,
        dq_comments = None,
    ) -> list:
        """ Create a list for the results from the DQ checks.

        Parameters
        ----------
        result : object
            A ExpectationValidationResult object.
        dq_name : DataFrame
            Name of the DQ check.
        dq_result : BaseDataContext
            Result from the DQ check.
        dq_row_count : DataFrame
            count of records from the DQ check.
        dq_unexpected_count : BaseDataContext
            unexpected count of records from the DQ check
        dq_unexpected_percent : BaseDataContext.
            unexpected percent of records from the DQ check.
        Returns
        -------
        DataFrame
            Dataframe for history and latest records which are loaded
        """
        dq_result = str(result['success'])

        if   dq_function_name == "dq_count_of_records_in_table" or dq_function_name == "dq_column_sum_value" :
            result_value = str(result['result']['observed_value'])
            dq_mostly = dq_mostly
            dq_range = f" range : [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
            dq_comments = f" '{dq_column_name} ' : records_count :-> {result['result']['observed_value']}, and range value :-> [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"

        else : 
            if dq_function_name == "dq_count_for_unique_values_in_columns":
                result_value = str(result['result']['element_count'] - result['result']['unexpected_count'] - result['result']['missing_count'])
            else:
                result_value = str(result['result']['element_count'] - result['result']['unexpected_count'])
            dq_mostly = result['expectation_config']['kwargs']['mostly']
            dq_range = f' range : [{dq_min_value}, {dq_min_value}]'
            dq_comments = f" '{dq_column_name} ': total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']}"

        self.result_list.append(
                (dq_function_name, result_value, dq_mostly, dq_range, dq_result, dq_comments))

        return self

 


    def dq_validate_compond_column_unique_values(self,
        col_list : list,
        mostly :float,                    
        )-> ExpectationValidationResult:
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_list : list
            hold list of result for result list function
        mostly   : float
            must be a float between 0 and 1. evaluates it as a percentage to fail or pass the validation
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly<0.1 or mostly>1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
              
            if not col_list:
                raise ValueError("Given list is empty, Please enter valid values")
                
            col_names = ","
            result =  self.validator.expect_compound_columns_to_be_unique(col_list, mostly, result_format = "SUMMARY")
            col_names = col_names.join(col_list)
            self.__get_result_list(result= result, dq_column_name = col_names,  dq_function_name = "dq_count_for_unique_values_in_compond_columns")
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    def dq_validate_row_count(self,
        min_value : int,
        max_value :int,
        )-> ExpectationValidationResult:
        """Create function to Assert Assert row count.
        Parameters
        ----------
        min_value : int
            count of the row in the table
        max_value : int
            count of the row in the table
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            result = self.validator.expect_table_row_count_to_be_between(min_value, max_value, result_format = "SUMMARY")
            self.__get_result_list(result= result, dq_function_name = "dq_count_of_records_in_table")
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    def dq_validate_column_values_to_not_be_null(self,
        col_name : str,
        mostly : float,
        )-> ExpectationValidationResult:
        """Create function to check null percentage for a column in DF
        Parameters
        ----------
        col_name : str
            Name of the column on which
        mostly :float
            thrashold value to validate the test cases
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly<0.1 or mostly>1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
                
            result =  self.validator.expect_column_values_to_not_be_null(col_name, mostly, result_format = "SUMMARY")
            self.__get_result_list(result= result, dq_column_name =  col_name, dq_function_name = "dq_not_null_records_percentage_for_column")
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    def dq_validate_range_for_numeric_column_sum_values(self,
        col_name : str,
        min_value : int,
        max_value : int,
        )-> ExpectationValidationResult:
        """Create function to check null percentage for a column in DF
        Parameters
        ----------
        col_name : str
            Name of the column on which 
        min_value : int
            count of the row in the table
        max_value : int
            count of the row in the table
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            result =  self.validator.expect_column_sum_to_be_between(col_name, min_value, max_value, result_format = "SUMMARY")
            self.__get_result_list(result= result, dq_column_name =  col_name, dq_function_name = "dq_column_sum_value")
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)



    def dq_validate_column_unique_values(self,
        col_name : str,
        mostly :int,
        )-> ExpectationValidationResult:
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_name : str
            Name of the column on which 
        mostly :int
            thrashold value to validate the test cases
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly<0.1 or mostly>1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
                
            result =  self.validator.expect_column_values_to_be_unique(col_name, mostly, result_format = "SUMMARY")
            self.__get_result_list( result=result,dq_column_name =  col_name, dq_function_name = "dq_count_for_unique_values_in_columns")
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    def dq_validate_count_variation_from_previous_version_values(self,
        target_location: str,
        min_value : int,
        max_value : int,
        older_version : int,
        latest_version : int
        )-> ExpectationValidationResult:
        """Create function to Assert column value is not null.
        Parameters
        ----------
        min_value : int
            count of the row in the table
        max_value : int
            count of the row in the table
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        result : object
            A DeltaTable object.
        """
        try:
            current_df, history_df = self.__get_delta_tables_history_dataframe(target_location = target_location, older_version=older_version,latest_version=latest_version)
            history_load_count = history_df.count()
            Latest_validator = ge.dataset.SparkDFDataset(current_df)
            result = Latest_validator.expect_table_row_count_to_be_between(min_value, max_value, result_format = "SUMMARY")
            dq_result = str(result['success'])
            dq_function_name = "dq_validate_count_variation_from_previous_version_values"
            result_value = result['result']['observed_value'] - history_load_count
            dq_comments = f" total_record_count_in_history :-> {history_load_count} , total_record_count_in_Latest :-> {result['result']['observed_value']}"
            dq_range = f" range : [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
            dq_mostly = None 
            if result['expectation_config']['kwargs']['min_value'] <= result_value <= result['expectation_config']['kwargs']['max_value']:
                dq_result = "True"
            self.result_list.append((dq_function_name, result_value, dq_mostly, dq_range, dq_result, dq_comments))     
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    def dq_validate_null_percentage_variation_from_previous_version_values(self,
        target_location: str,
        col_name : str,
        mostly : float,
        older_version : int,
        latest_version : int
        )-> ExpectationValidationResult:

        """Create function to Assert column value is not null.
        Parameters
        ----------
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        results: object
            A DeltaTable object.
        col_name : str
            Name of the column on which 
        mostly  : float
            threshold value to validate the test cases
        """
        try:
            if mostly<0.1 or mostly>1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
                
            dq_min_value = None
            dq_min_value = None
            current_df, history_df = self.__get_delta_tables_history_dataframe(target_location = target_location,older_version=older_version,latest_version=latest_version)
            history_validator = ge.dataset.SparkDFDataset(history_df)
            Latest_validator = ge.dataset.SparkDFDataset(current_df)
            history_result = history_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
            current_result = Latest_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
            dq_result = str(current_result['success'])
            result_value = round(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'], 2)
            dq_function_name  = "dq_validate_null_percentage_variation_values"
            dq_comments = f" '{col_name}' : null percentage in history :-> {history_result['result']['unexpected_percent']:.2f} , null percentage in latest :-> {current_result['result']['unexpected_percent']:.2f}"
            dq_range = f' range : [{dq_min_value}, {dq_min_value}]'
            cal_mostly = round(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'], 2)
            if(cal_mostly <= mostly):
                dq_result = "True"
            self.result_list.append((dq_function_name, result_value, mostly, dq_range, dq_result, dq_comments))      
            return self
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)