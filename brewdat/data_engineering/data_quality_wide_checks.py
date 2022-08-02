import datetime
import great_expectations as ge
from ruamel import yaml
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.core import ExpectationValidationResult
from great_expectations.dataset import SparkDFDataset
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults
from . import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils


    
def get_delta_tables_history_dataframe(
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
        A ExpectationValidationResult object.
    Returns
    -------
    DataFrame
        Dataframe for history and latest records which are loaded in delta table
    """
    try:
        latest = results.delta_table.history().filter(F.col("operation") == "WRITE").select(F.col("version").cast("int")).first()[0]
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
    dq_total_records = None,
    dq_unexpected_records = None,
    dq_unexpected_percent = None,
    dq_function_name = None,
    dq_comments = None
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
    
    if   dq_function_name == "dq_validate_row_count" :
        dq_total_records = str(result['result']['observed_value'])
        dq_unexpected_records = str(result['result']['observed_value']-
                                    result['expectation_config']['kwargs']['value'])
        dq_unexpected_percent = str(result['result']['observed_value'] * 
                                    result['expectation_config']['kwargs']['value'] / 100)
     
    elif   dq_function_name == "dq_count_variation_from_previous_version" :
        dq_total_records = str(result['result']['observed_value'])
        dq_unexpected_records = str(result['result']['observed_value']-
                                    result['expectation_config']['kwargs']['value'])
        dq_unexpected_percent = str(result['result']['observed_value'] *
                                    result['expectation_config']['kwargs']['value'] /100)
        
    elif dq_function_name == "dq_validate_range_for_numeric_column_sum_values" :
        dq_total_records = str(result['result']['element_count'])
        dq_comments = 'failed : actual sum value -> ' \
                            + str(result['result']['observed_value']) \
                            + ', range specified min_values-> ' \
                            + str(result['expectation_config']['kwargs']['min_value']) \
                            + ' ,  max_values ->' + str(result['expectation_config']['kwargs']['max_value'])     
    else :
        dq_total_records = str(result['result']['element_count'])
        dq_unexpected_records = str(result['result']['unexpected_count'])
        dq_unexpected_percent = str(result['result']['unexpected_percent'])[0:5]
  
    resultlist.append(
            (dq_function_name, dq_total_records, dq_unexpected_records, dq_unexpected_percent, dq_result, dq_comments ))
    
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
                                               StructField('dq_total_records',StringType()),
                                               StructField('dq_unexpected_records',StringType()),
                                               StructField('dq_unexpected_percent',StringType()),
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
    col_names : list,
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
    """
    try:
        result =  validator.expect_compound_columns_to_be_unique(col_names, mostly, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_validate_compond_column_unique_values")
        print(result_list)
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
     
    
def dq_validate_row_count(
    dbutils: object, 
    validator: Validator,
    row_count : int,
    mostly :int,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert Assert row count.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    row_count : int
        count of the row in the table
    """
    try:
        result = validator.expect_table_row_count_to_equal(row_count, mostly, result_format = "SUMMARY")
        print(result)
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_validate_row_count")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

    
def dq_validate_column_nulls_values(
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
    """
    try:
        result =  validator.expect_column_values_to_not_be_null(col_name, mostly, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_validate_column_nulls_values")
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
    """
    try:
        result =  validator.expect_column_sum_to_be_between(col_name, min_value, max_value, result_format = "SUMMARY")
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_validate_range_for_numeric_column_sum_values")
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
    """
    try:
        result =  validator.expect_column_values_to_be_unique(col_name, mostly, result_format = "SUMMARY")
        dresult_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_validate_column_unique_values")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)



def dq_validate_count_variation_from_previous_version_values(
    dbutils: object, 
    current_validator: Validator,
    history_df : DataFrame,
    current_df : DataFrame,
    resultlist : list = [])-> ExpectationValidationResult:
    """Create function to Assert column value is not null.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_name : str
        Name of the column on which 
    """
    try:
        history_load_count = history_df.count()
        Latest_validator = ge.dataset.SparkDFDataset(current_df)
        result = Latest_validator.expect_table_row_count_to_equal(history_load_count, result_format = "SUMMARY")
        print(result)
        result_list = __get_result_list(result= result, resultlist= resultlist, dq_function_name = "dq_count_variation_from_previous_version")
        return result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

        
def dq_validate_null_percentage_variation_from_previous_version_values(
    dbutils: object, 
    history_df: DataFrame,
    current_df: DataFrame,
    col_name : str,
    resultlist : list = [])-> ExpectationValidationResult:
    
    """Create function to Assert column value is not null.
    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    validator : Validator
        Name of the Validator object.
    col_name : str
        Name of the column on which 
    """
    try:
        history_validator = ge.dataset.SparkDFDataset(history_df)
        Latest_validator = ge.dataset.SparkDFDataset(current_df)
        history_result = history_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
        current_result = Latest_validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
        dq_total_records = ' records before ingestion --> ' \
                            + str(history_result['result']['element_count']) \
                            + ' records after ingestion --> ' \
                            + str(current_result['result']['element_count']) 
        dq_unexpected_records = ' unexpected_records before ingestion --> ' \
                            + str(history_result['result']['unexpected_count']) \
                            + ' unexpected_records after ingestion --> ' \
                            + str(current_result['result']['unexpected_count']) 
        dq_unexpected_percent = ' unexpected_records before ingestion --> ' \
                            + str(history_result['result']['unexpected_percent'])[0:5] \
                            + ' unexpected_records after ingestion --> ' \
                            + str(current_result['result']['unexpected_percent'])[0:5] 
        dq_function_name  = "dq_validate_null_percentage_variation_from_previous_version_values"
        dq_result = str(current_result['success'])
        dq_comments = 'null variation -->' + str(history_result['result']['unexpected_percent'] - current_result['result']['unexpected_percent'])[0:4]
        resultlist.append((dq_function_name, dq_total_records, dq_unexpected_records, dq_unexpected_percent, dq_result, dq_comments ))        
        return resultlist
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

