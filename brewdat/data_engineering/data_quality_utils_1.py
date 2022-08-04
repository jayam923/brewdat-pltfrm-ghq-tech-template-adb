import re
import ast
import pandas as pd
import pyspark
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StringType,StructField
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col, count, lit, length, when, array_union, array
from pyspark.sql.types import IntegerType,DecimalType,ByteType,StringType,LongType,BooleanType,DoubleType,FloatType
from pyspark.sql import SparkSession
from pyspark.sql import Window
from typing import List
from pyspark.context import SparkContext
from datetime import datetime as dt
from . import common_utils, transform_utils



def __get_col_list(src_df:DataFrame)-> List :
    fields_list= [x for x in src_df.columns]
    return fields_list

def __get_lower_Case(values:list)-> List :
    fields_list= [x.lower() for x in values]
    return fields_list

def data_type_check(
    field_name : str, 
    data_type : str, 
    src_df:DataFrame )->DataFrame:
    
    """Checks the field datatype for field present at ith position of the
    validation dataframe.
    Parameters
    ----------
    field_name : str
        Column name to test values.
    data_type : str
        Column data type.
    src_df : DataFrame
        PySpark DataFrame to modify.
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        #fields_list = get_col_list(src_df)
        df_final=None
        if data_type == "byte" :
            df_final = src_df.withColumn(f'{field_name}_type',col(field_name)
                .cast(ByteType()))

        elif data_type== "Integer" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(IntegerType()))

        elif data_type== "string" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(StringType()))

        elif data_type== "bigint" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(LongType()))

        elif data_type== "bool" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(BooleanType()))

        elif data_type== "decimal" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(DecimalType(12,5)))

        elif data_type== "float" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(FloatType()))

        elif data_type== "double" : 
            df_final=src_df.withColumn(f'{field_name}_type',col(field_name)\
                .cast(DoubleType()))

        if df_final is None:
            return src_df
        else:
            src_df = (
                     df_final.withColumn('__bad_record',
                     when((col(f'{field_name}_type').isNull()) & (col(field_name).isNotNull()),
                     lit('True')).otherwise(col('__bad_record')))
            )
            src_df = (
                     src_df.withColumn('__data_quality_issues',
                     when((col(f'{field_name}_type').isNull()) & (col(field_name).isNotNull()),
                     array_union('__data_quality_issues',
                     array(lit(f' {field_name} ; Data type mismatch'))))
                     .otherwise(col('__data_quality_issues')))
                     .withColumn("dq_run_timestamp",f.current_timestamp())
                     .drop(col(f'{field_name}_type'))
            )
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def null_check(
    field_name : str, 
    src_df:DataFrame )->DataFrame:           
    
    """Helps to validate null values in the column
    Parameters
    ----------
    field_name : str
        Column name to test values.
    src_df : DataFrame
        PySpark DataFrame to modify.

    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """

    try:
        src_df = src_df.withColumn('__bad_record',when(col(field_name).isNull(),
                                   lit('True')).otherwise(col('__bad_record')))

        src_df = src_df.withColumn('__data_quality_issues',
                           when(col(field_name).isNull(),
                           array_union('__data_quality_issues',
                           array(lit(f' {field_name} ; Records contain null values'
                           )))).otherwise(col('__data_quality_issues'
                           ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils) 


def max_length(
    field_name : str,
    maximum_length : int,
    src_df:DataFrame )->DataFrame:
    
    """Checks the field column length against the min and max values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    maximum_length : int
        maximum length column values.
    df : DataFrame
        PySpark DataFrame to modify.
            
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
        
    """
    try:
        src_df = src_df.withColumn('__bad_record', when(length(field_name)
                               > maximum_length, lit('True'
                               )).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues', when(length(field_name)
                               > maximum_length,
                               array_union('__data_quality_issues',
                               array(lit(f' {field_name} ; This records exceed the max_length defined for the column'
                               )))).otherwise(col('__data_quality_issues'
                               ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def min_length(
    field_name : str,
    minimum_length : int,
    src_df:DataFrame )->DataFrame:
    """Checks the field column length against the min and max values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    minimum_length : int
        minimum length column values.
    src_df : DataFrame
        PySpark DataFrame to modify.
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        src_df = src_df.withColumn('__bad_record', when(length(field_name)
                           < minimum_length, lit('True'
                           )).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues', when(length(field_name)
                           < minimum_length,
                           array_union('__data_quality_issues',
                           array(lit(f' {field_name} ; This records lenght is lesser then then min_length defined for the column'
                           )))).otherwise(col('__data_quality_issues'
                           ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)    

    
def range_value(
    field_name : str, 
    minimum_value : int, 
    maximum_value : int, 
    src_df:DataFrame )->DataFrame:
    """Checks the field column values against between min and max values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    minimum value : int
        minimum value column values.
    maximum value : int
        maximum value column values.
    src_df : DataFrame
        PySpark DataFrame to modify.
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        src_df = src_df.withColumn('__bad_record',
                               when(col(field_name).between(minimum_value,
                               maximum_value), lit('True'
                               )).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues',
                               when(col(field_name).between(minimum_value,
                               maximum_value),
                               array_union('__data_quality_issues',
                               array(lit(f' {field_name} ; This records does not lie in between the specified range'
                               )))).otherwise(col('__data_quality_issues'
                               ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

    
def invalid_values(
    field_name : str,
    invalid_values,
    src_df:DataFrame )->DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    invalid_values : list
        List of values which are invalid
    src_df : DataFrame
        The src_df DataFrame needed to verify
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        list_values = invalid_values
        src_df = src_df.withColumn('__bad_record',
                           when(col(field_name).isin(list(map(lambda x: x , list_values))),
                           lit('True')).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues',
                           when(col(field_name).isin(list(map(lambda x: x , list_values))),
                           array_union('__data_quality_issues',
                           array(lit(f' {field_name} ; This record is having invalid values'
                           )))).otherwise(col('__data_quality_issues'
                           ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
    
    
def valid_values(
    field_name : str,
    valid_values,
    src_df:DataFrame)->DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    valid_values : list
        List of values which are valid
    src_df : DataFrame
        The src_df DataFrame needed to verify
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        list_values = valid_values
        src_df = src_df.withColumn('__bad_record',
                               when(col(field_name).isin(list(map(lambda x: x , list_values)))
                               == False, lit('True'
                               )).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues',
                               when(col(field_name).isin(list(map(lambda x: x , list_values)))
                               == False, array_union('__data_quality_issues',
                               array(lit(f'{field_name} ; This record is having invalid values'
                               )))).otherwise(col('__data_quality_issues'
                               ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def valid_regular_expression(
    field_name : str,
    valid_regular_expression : str,
    src_df:DataFrame)->DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    src_df : DataFrame
        The src_df DataFrame needed to verify
    field_name : DataFrame
        The src_df DataFrame needed to verify
    valid_regular_expression : str
        Regex to filter the data
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        exp= valid_regular_expression
        src_df = src_df.withColumn('__bad_record',
                           when(src_df[field_name].rlike(exp),
                           lit('True')).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues',
                           when(src_df[field_name].rlike(exp),
                           array_union('__data_quality_issues',
                           array(lit(f' {field_name} ; This record does not align based on the regex given'
                           )))).otherwise(col('__data_quality_issues'
                           ))).withColumn("dq_run_timestamp",f.current_timestamp())
        return src_df
    except Exception:
            common_utils.exit_with_last_exception(dbutils)


def duplicate_check(
    col_list : str,
    src_df:DataFrame)->DataFrame:
    
    """Checks the field column values against valid values
    Parameters
    ----------
    col_list : list
        List of columns on based on which we need to check the duplicate values
    src_df : DataFrame
        The src_df DataFrame needed to verify
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    try:
        w = Window.partitionBy(col_list)
        src_df = src_df.select('*', f.count('*').over(w).alias('Duplicate_indicator'))
        src_df = src_df.withColumn('__bad_record',
                               when(col('Duplicate_indicator') > 1,
                               lit('True')).otherwise(col('__bad_record')))
        src_df = src_df.withColumn('__data_quality_issues',
                               when(col('Duplicate_indicator') > 1,
                               array_union('__data_quality_issues',
                               array(lit('This records contain duplicate values'
                               )))).otherwise(col('__data_quality_issues'
                               ))).withColumn("dq_run_timestamp",f.current_timestamp()).drop('Duplicate_indicator')
        return src_df
    except Exception:
            common_utils.exit_with_last_exception(dbutils)
    
def column_check(
    col_list,
    src_df:DataFrame)-> List:
    
    """Checks the field column values against valid values
    Parameters
    ----------
    
    col_list : list
        List of columns name to verify
    src_df : DataFrame
        The src_df DataFrame needed to verify
            
    Returns
    -------
    missing_fields
        [List]: List with column name 
    """
    try:
        missing_fields = [] 
        fields_list =  __get_lower_Case(__get_col_list(src_df))
        col_list = __get_lower_Case(col_list)
        for i in range(0,len(col_list)):
            if col_list[i] not in fields_list:
                missing_fields.append(col_list[i])
        if(len(missing_fields)== 0):
            print("All required columns are present")
        else :
            print("list of missing column names are - > ")
            print(missing_fields)
        return missing_fields
    except Exception:
            common_utils.exit_with_last_exception(dbutils)
    

def run_validation(
    spark: SparkSession,
    dbutils: object,
    src_df : DataFrame,
    json_df : DataFrame)-> DataFrame:
    
    """Checks the field column values against valid values
    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    dbutils : object
        A Databricks utils object.
    src_df : DataFrame
        The src_df file needed to verify
    json_df : DataFrame
        The logic_df DataFrame to get the input values from json file
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    logic_df=json_df.toPandas()
    logic_df["field_name"]=logic_df["field_name"].str.lower()
    for col in src_df.columns:
        src_df = src_df.withColumnRenamed(col,col.lower())
    col_list=logic_df['field_name'].tolist()
    missing_fields = column_check(col_list,src_df )
    if len(missing_fields)!=0:
        print('column missing',missing_fields)
        raise ValueError('Souce Columns are not matching with Metadata')
    else:
        #Get PK value as list from config_rules run all the validations
        print('Duplicate check started')
        Pk_col_list=[]
        Pk_col_list=logic_df[logic_df['primary_key']=='Yes']["field_name"].tolist()
        #Check Unquie/duplcate records
        if len(Pk_col_list)==0:
            pass
        else:
            src_df=duplicate_check(Pk_col_list,src_df)
        for i in range(0,logic_df.shape[0]):
            if logic_df.loc[i,"field_name"] in src_df.columns:
                print(logic_df.loc[i,"field_name"])
                ## Mandatory Checks whether null values present or not
                if pd.notnull(logic_df.loc[i,'is_null']):
                    print('null_check started')
                    src_df = null_check(logic_df.loc[i,"field_name"],src_df)
                    ## DataType checks
                if pd.notnull(logic_df.loc[i,'data_type']):
                    print('data_type_check check started')
                    src_df= data_type_check(logic_df.loc[i,"field_name"],logic_df.loc[i,"data_type"], src_df)
                else:
                    print('No Datatype')
                 ## range checks
                if pd.notnull(logic_df.loc[i,'maximum_value']):
                    if pd.notnull(logic_df.loc[i,'minimum_value']):
                        print('data_range_check check started')
                        src_df= range_value(logic_df.loc[i,"field_name"], logic_df.loc[i,"minimum_value"] ,logic_df.loc[i,"maximum_value"],src_df)
                else:
                    print('No Maximum_value and Minimum_value')
                ##Passing only not null values for remaining checks
                    ## checking for Max values
                if pd.notnull(logic_df.loc[i,'maximum_length']):
                    print('max_length check started')
                    src_df= max_length(logic_df.loc[i,"field_name"],  logic_df.loc[i,"maximum_length"],src_df)
                else:
                    pass

                ## checking for Min values
                if pd.notnull(logic_df.loc[i,'minimum_length']):
                    print('min_length check started')
                    src_df = min_length(logic_df.loc[i,"field_name"], logic_df.loc[i,"minimum_length"],src_df)
                else:
                    pass
                ## Checking the Valid values field
                if str(logic_df.loc[i,'valid_values']) != "None" :
                    print(logic_df.loc[i,'valid_values'])
                    print('valid_values check started')
                    src_df =valid_values(logic_df.loc[i,"field_name"], logic_df.loc[i,"valid_values"],src_df)
                else:
                    pass
                ## Checking the Invalid values field
                if str(logic_df.loc[i,'invalid_values']) != "None" :
                    print('Invalid_values check started')
                    src_df =invalid_values(logic_df.loc[i,"field_name"],logic_df.loc[i,"invalid_values"],src_df)
                else:
                    pass

                ## Valid regular expression check filed
                if pd.notnull(logic_df.loc[i,'valid_regular_expression']):
                    print('Valid_Regular_Expression check started')
                    src_df =valid_regular_expression(logic_df.loc[i,"field_name"], logic_df.loc[i,"valid_regular_expression"],src_df)
                else:
                    pass
            else:
                pass

        print('completed')
        return src_df