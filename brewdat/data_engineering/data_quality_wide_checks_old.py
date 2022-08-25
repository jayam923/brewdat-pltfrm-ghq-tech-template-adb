import datetime
import math
import great_expectations as ge
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,FloatType
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core import ExpectationValidationResult
from great_expectations.validator.validator import Validator
from . import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils


    
def __get_delta_tables_history_dataframe(
    spark: SparkSession, 
    dbutils: object, 
    target_location: str, 
    results : object ) -> DataFrame:
    """Create an object for data context for accessing all of the primary methods for creating elements of your project related to DQ checks.
    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    dbutils : object
        A Databricks utils object.
    target_location : str
        Absolute Delta Lake path for the physical location of this delta table.
    result : object
        A DeltaTable object.
    Returns
    -------
    DataFrame
        Dataframe for history and latest records which are loaded in delta table
    """
    try:
        latest = results.delta_table.history().filter(f.col("operation") == "WRITE").select(f.col("version").cast("int")).first()[0]
        history = latest-4
        latest_df = spark.read.format("delta").option("versionAsOf", latest).load(target_location)
        history_df = spark.read.format("delta").option("versionAsOf", history).load(target_location)
        return latest_df, history_df
    
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def configure_great_expectation(
    df: DataFrame) -> SparkDFDataset:
    """Create an object for data context for accessing all of the primary methods for creating elements of your project related to DQ checks.
    
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to write.
        
    Returns
    -------
    SparkDFDataset
        SparkDFDataset object for great expectation
    """
    try:
        validator = ge.dataset.SparkDFDataset(df)
        result_list = []
        return validator, result_list
    
    except Exception:
        common_utils.exit_with_last_exception(dbutils)    

        
def __get_result_list(
    result : ExpectationValidationResult,
    resultlist : list =[],
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
        dq_comments = f" {dq_column_name} -: records_count :-> {result['result']['observed_value']}, and range value :-> [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
     
    else :  
        result_value = str(result['result']['element_count'] - result['result']['unexpected_count'])
        dq_mostly = result['expectation_config']['kwargs']['mostly']
        dq_range = f' range : [{dq_min_value}, {dq_min_value}]'
        dq_comments = f" {dq_column_name} -: total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']}"
        
    resultlist.append(
            (dq_function_name, result_value, dq_mostly, dq_range, dq_result, dq_comments))
    
    return resultlist
 

def get_wider_dq_results( 
    spark: SparkSession, 
    dbutils: object, 
    values : list,
    with_history = None) -> DataFrame:
    """Create an object for data context for accessing all of the primary methods for creating elements of your project related to DQ checks.
    
    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    dbutils : object
        A Databricks utils object.
    values : list
        List of values from the result json 
    
    Returns
    -------
    DataFrame
        Dataframe for history and latest records which are loaded
    """
    try:
        result_schema = (
             StructType(fields=[StructField('dq_function_name',StringType()),
                                               StructField('result_value',StringType()), 
                                               StructField('dq_mostly',FloatType()),
                                               StructField('dq_range',StringType()),
                                               StructField('dq_result',StringType()),
                                               StructField('dq_comments',StringType())])
             )                   
        result_df= spark.createDataFrame(values, result_schema)
        return result_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)    
        
         
def dq_validate_compond_column_unique_values(
    dbutils: object, 
    validator: Validator,
    col_list : list,
    mostly :int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert if column has unique values.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_list : list
        hold list of result for result list function
    col_name : str
        must be a float between 0 and 1. evaluates it as a percentage to fail or pass the validation
    resultlist : list
        takes the list to hold the result in result df
    Returns
    -------
    result
        ExpectationValidationResult object
    """
    try:
        col_names = " "
        result =  validator.expect_compound_columns_to_be_unique(col_list, mostly, result_format = "SUMMARY")
        col_names = col_names.join(col_list)
        print(col_names)
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_column_name = col_names,  dq_function_name = "dq_count_for_unique_values_in_compond_columns")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
     
    
def dq_validate_row_count(
    dbutils: object, 
    validator: Validator,
    min_value : int,
    max_value :int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert Assert row count.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    min_value : int
        count of the row in the table
    max_value : int
        count of the row in the table
    resultlist : list
        takes the list to hold the result in result df
    Returns
    -------
    result
        ExpectationValidationResult object
    """
    try:
        result = validator.expect_table_row_count_to_be_between(min_value, max_value, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_count_of_records_in_table")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

    
def dq_validate_column_values_to_not_be_null(
    dbutils: object, 
    validator: Validator, 
    col_name : str,
    mostly :int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to check null percentage for a column in DF
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_name : str
        Name of the column on which
    mostly :int
        thrashold value to validate the test cases
    resultlist : list
        takes the list to hold the result in result df
    Returns
    -------
    result
        ExpectationValidationResult object
    """
    try:
        result =  validator.expect_column_values_to_not_be_null(col_name, mostly, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_column_name =  col_name, dq_function_name = "dq_not_null_records_percentage_for_column")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        
        
def dq_validate_range_for_numeric_column_sum_values(
    dbutils: object, 
    validator: Validator, 
    col_name : str,
    min_value : int,
    max_value : int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to check null percentage for a column in DF
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_name : str
        Name of the column on which 
    min_value : int
        count of the row in the table
    max_value : int
        count of the row in the table
    resultlist : list
        takes the list to hold the result in result df
    Returns
    -------
    result
        ExpectationValidationResult object
    """
    try:
        result =  validator.expect_column_sum_to_be_between(col_name, min_value, max_value, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_column_name =  col_name, dq_function_name = "dq_column_sum_value")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        
                           
    
def dq_validate_column_unique_values(
    dbutils: object, 
    validator: Validator, 
    col_name : str,
    mostly :int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert if column has unique values.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_name : str
        Name of the column on which 
    mostly :int
        thrashold value to validate the test cases
    resultlist : list
        takes the list to hold the result in result df
    Returns
    -------
    result
        ExpectationValidationResult object
    """
    try:
        result =  validator.expect_column_values_to_be_unique(col_name, mostly, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_column_name =  col_name, dq_function_name = "dq_count_for_unique_values_in_columns")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def dq_validate_count_variation_from_previous_version_values(
    spark: SparkSession,
    dbutils: object, 
    target_location: str,
    results : object,
    min_value : int,
    max_value : int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert column value is not null.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    target_location : str
        Absolute Delta Lake path for the physical location of this delta table.
    result : object
        A DeltaTable object.
    resultlist : list
        takes the list to hold the result in result df
    """
    try:
        current_df, history_df = __get_delta_tables_history_dataframe(spark = spark, dbutils =dbutils,  target_location = target_location, results = results)
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
        resultlist.append((dq_function_name, result_value, dq_mostly, dq_range, dq_result, dq_comments))     
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

        
def dq_validate_null_percentage_variation_from_previous_version_values(
    spark: SparkSession,
    dbutils: object, 
    target_location: str,
    results : object,
    col_name : str,
    dq_mostly : int,
    resultlist : list = [])-> ExpectationValidationResult:
    
    """Create function to Assert column value is not null.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    target_location : str
        Absolute Delta Lake path for the physical location of this delta table.
    result : object
        A DeltaTable object.
    col_name : str
        Name of the column on which 
    resultlist : list
        takes the list to hold the result in result df
    """
    try:
        dq_min_value = None
        dq_min_value = None
        current_df, history_df = __get_delta_tables_history_dataframe(spark = spark, dbutils =dbutils,  target_location = target_location, results = results)
        history_validator = ge.dataset.SparkDFDataset(history_df)
        Latest_validator = ge.dataset.SparkDFDataset(current_df)
        history_result = history_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
        current_result = Latest_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
        dq_result = str(current_result['success'])
        result_value = round(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'], 2)
        dq_function_name  = "dq_validate_null_percentage_variation_values"
        dq_comments = f" {col_name} -; null percentage in history :-> {history_result['result']['unexpected_percent']:.2f}  null percentage in latest :-> {current_result['result']['unexpected_percent']:.2f}"
        dq_range = f' range : [{dq_min_value}, {dq_min_value}]'
        cal_mostly = round(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'], 2)
        dq_mostly = dq_mostly
        if(cal_mostly <= dq_mostly):
            dq_result = "True"
        resultlist.append((dq_function_name, result_value, dq_mostly, dq_range, dq_result, dq_comments))      
        return resultlist
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        
        
