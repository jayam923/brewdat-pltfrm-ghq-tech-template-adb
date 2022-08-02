import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, lit, length, when, array_union, array
from pyspark.sql.types import IntegerType,DecimalType,ByteType,StringType,LongType,BooleanType,DoubleType,FloatType
from pyspark.sql import SparkSession
from pyspark.sql import Window, Column
from typing import List
from . import common_utils


def create_required_columns_for_dq_check(df:DataFrame )->DataFrame:
    """To create required columns to maintain DQ checks.
    Parameters
    ----------
    src_df : DataFrame
        PySpark DataFrame to modify.
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with required columns.
    """
    dq_dataframe = df.withColumn('__bad_record', lit('False')).withColumn('__data_quality_issues',array())
    return dq_dataframe

            
def data_type_check(
    df: DataFrame,
    field_name : str,
    data_type : str,
    dbutils: object,
) -> DataFrame:
    
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
        df_final=None
        if data_type == "byte" :
            df_final = df.withColumn("data_type_test",col(field_name)\
                                         .cast(ByteType()))

        elif data_type== "Integer" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                                       .cast(IntegerType()))

        elif data_type== "String" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                                       .cast(StringType()))

        elif data_type== "bigint" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                .cast(LongType()))

        elif data_type== "bool" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                .cast(BooleanType()))

        elif data_type== "decimal" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                .cast(DecimalType(12,5)))

        elif data_type== "float" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                .cast(FloatType()))

        elif data_type== "double" : 
            df_final=df.withColumn("data_type_test",col(field_name)\
                .cast(DoubleType()))
        if df_final is None:
            return df
        else:
            return __perform_dq_check(
                df=df_final,
                condition=(F.col("data_type_test").isNull()) & (F.col(field_name).isNotNull()),
                dq_failure_message=f'{field_name}: Data type mismatch.',
                dbutils=dbutils)
    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def null_check(
        df: DataFrame,
        field_name: str,
        dbutils: object,
) -> DataFrame:
    
    """Helps to validate null values in the column
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to modify.
    field_name : str
        Column name to test values.
    dbutils : object
        A Databricks utils object.

    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    return __perform_dq_check(
        df=df,
        condition=F.col(field_name).isNull(),
        dq_failure_message=f'{field_name}: record contain null value.',
        dbutils=dbutils)

    


def max_length(
        df: DataFrame,
        field_name: str,
        maximum_length: int,
        dbutils: object,
) -> DataFrame:
    
    """Checks the field column length against the min and max values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    maximum_length : int
        maximum length column values.
    df : DataFrame
        PySpark DataFrame to modify.
    dbutils : object
        A Databricks utils object.        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
        
    """
    return __perform_dq_check(
        df=df,
        condition=(F.length(field_name) > maximum_length),
        dq_failure_message=f'{field_name}: max length of {maximum_length} exceeded.',
        dbutils=dbutils)


def min_length(
    field_name : str,
    minimum_length : int,
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
    """Checks the field column length against the min and max values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    minimum_length : int
        minimum length column values.
    src_df : DataFrame
        PySpark DataFrame to modify.
    dbutils : object
        A Databricks utils object.    
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    return __perform_dq_check(
        df=df,
        condition=(F.length(field_name) < minimum_length),
        dq_failure_message=f'{field_name}: min length of {minimum_length} exceeded.',
        dbutils=dbutils)
    

def range_value(
    field_name : str, 
    minimum_value : int, 
    maximum_value : int, 
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
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
    dbutils : object
        A Databricks utils object.    
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    return __perform_dq_check(
        df=df,
        condition=F.col(field_name).between(minimum_value, maximum_value) == False,
        dq_failure_message=f'{field_name}: range value does not lie in between minimum value -> {minimum_value} and maximum value -> {maximum_value} value.',
        dbutils=dbutils)
    
    
def invalid_values(
    field_name : str,
    invalid_values,
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    invalid_values : list
        List of values which are invalid
    src_df : DataFrame
        The src_df DataFrame needed to verify
    dbutils : object
        A Databricks utils object.
        
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    
    return __perform_dq_check(
        df=df,
        condition=F.col(field_name).isin(list(map(lambda x: x , invalid_values))),
        dq_failure_message=f'{field_name}: this record comes under invalid values  -> {invalid_values}.',
        dbutils=dbutils)

    
def valid_values(
    field_name : str,
    valid_values,
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    field_name : str
        Column name to test values.
    valid_values : list
        List of values which are valid
    src_df : DataFrame
        The src_df DataFrame needed to verify
    dbutils : object
        A Databricks utils object.    
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    return __perform_dq_check(
        df=df,
        condition=F.col(field_name).isin(list(map(lambda x: x , valid_values)))== False,
        dq_failure_message=f'{field_name}: this record is not in list of valid values  -> {valid_values}.',
        dbutils=dbutils)


def valid_regular_expression(
    field_name : str,
    regex : str,
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
    """Checks the field column values against valid values
    Parameters
    ----------
    src_df : DataFrame
        The src_df DataFrame needed to verify
    field_name : DataFrame
        The src_df DataFrame needed to verify
    valid_regular_expression : str
        Regex to filter the data
    dbutils : object
        A Databricks utils object.    
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    
    return __perform_dq_check(
        df=df,
        condition=F.col(field_name).rlike(regex) == False,
        dq_failure_message=f'{field_name}: does not align as per the given regex {regex}.',
        dbutils=dbutils)
    

def duplicate_check(
    col_list : str,
    df:DataFrame,
    dbutils: object,
) -> DataFrame:
    
    """Checks the field column values against valid values
    Parameters
    ----------
    col_list : list
        List of columns on based on which we need to check the duplicate values
    src_df : DataFrame
        The src_df DataFrame needed to verify
    dbutils : object
        A Databricks utils object.    
    Returns
    -------
    DataFrame: 
        Returns Pyspark Dataframe with bad record indicator and validation message.
    """
    w = Window.partitionBy(col_list)
    df = df.select('*', f.count('*').over(w).alias('Duplicate_indicator'))
    print(df)
    return __perform_dq_check(
        df=df,
        condition=F.col("Duplicate_indicator") > 1,
        dq_failure_message=f'This records contain duplicate values.',
        dbutils=dbutils)
    


def column_check(
    col_list,
    df:DataFrame,
    dbutils: object,
)-> List:
    
    """Checks the field column values against valid values
    Parameters
    ----------
    
    col_list : list
        List of columns name to verify
    src_df : DataFrame
        The src_df DataFrame needed to verify
    dbutils : object
        A Databricks utils object.        
    Returns
    -------
    missing_fields
        [List]: List with column name 
    """
    try:
        missing_fields = [] 
        fields_list =  __get_lower_case(__get_col_list(df))
        col_list = __get_lower_case(col_list)
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
    json_df : DataFrame,
)-> DataFrame:
    
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
    missing_fields = column_check(col_list, src_df, dbutils )
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
            src_df=duplicate_check(Pk_col_list,src_df, dbutils)
        for i in range(0,logic_df.shape[0]):
            if logic_df.loc[i,"field_name"] in src_df.columns:
                print(logic_df.loc[i,"field_name"])
                ## Mandatory Checks whether null values present or not
                if pd.notnull(logic_df.loc[i,'is_null']):
                    print('null_check started')
                    src_df = null_check(src_df, logic_df.loc[i,"field_name"], dbutils)
                    ## DataType checks
                if pd.notnull(logic_df.loc[i,'data_type']):
                    print('data_type_check check started')
                    src_df= data_type_check(src_df, logic_df.loc[i,"field_name"],logic_df.loc[i,"data_type"], dbutils)
                else:
                    print('No Datatype')
                 ## range checks
                if pd.notnull(logic_df.loc[i,'maximum_value']):
                    if pd.notnull(logic_df.loc[i,'minimum_value']):
                        print('data_range_check check started')
                        src_df= range_value(logic_df.loc[i,"field_name"], logic_df.loc[i,"minimum_value"] ,logic_df.loc[i,"maximum_value"], src_df, dbutils)
                else:
                    print('No Maximum_value and Minimum_value')
                ##Passing only not null values for remaining checks
                    ## checking for Max values
                if pd.notnull(logic_df.loc[i,'maximum_length']):
                    print('max_length check started')
                    src_df= max_length(logic_df.loc[i,"field_name"],  logic_df.loc[i,"maximum_length"],src_df, dbutils)
                else:
                    pass

                ## checking for Min values
                if pd.notnull(logic_df.loc[i,'minimum_length']):
                    print('min_length check started')
                    src_df = min_length(logic_df.loc[i,"field_name"], logic_df.loc[i,"minimum_length"],src_df, dbutils)
                else:
                    pass
                ## Checking the Valid values field
                if str(logic_df.loc[i,'valid_values']) != "None" :
                    print(logic_df.loc[i,'valid_values'])
                    print('valid_values check started')
                    src_df =valid_values(logic_df.loc[i,"field_name"], logic_df.loc[i,"valid_values"],src_df, dbutils)
                else:
                    pass
                ## Checking the Invalid values field
                if str(logic_df.loc[i,'invalid_values']) != "None" :
                    print('Invalid_values check started')
                    src_df =invalid_values(logic_df.loc[i,"field_name"],logic_df.loc[i,"invalid_values"],src_df, dbutils)
                else:
                    pass

                ## Valid regular expression check filed
                if pd.notnull(logic_df.loc[i,'valid_regular_expression']):
                    print('Valid_Regular_Expression check started')
                    src_df =valid_regular_expression(logic_df.loc[i,"field_name"], logic_df.loc[i,"valid_regular_expression"],src_df, dbutils)
                else:
                    pass
            else:
                pass

        print('completed')
        return src_df


def __perform_dq_check(
        df: DataFrame,
        condition: Column,
        dq_failure_message: str,
        dbutils: object,
)-> DataFrame:

    try:
        result_df = (
            df.withColumn("dq_run_timestamp", F.current_timestamp())
            .withColumn('__data_quality_issues',
                        when(condition,F.array_union('__data_quality_issues',F.array(lit(dq_failure_message))))
                        .otherwise(F.col('__data_quality_issues'))
                        )
            .withColumn("__bad_record", F.size("__data_quality_issues") > 0)

        )
        if "data_type_test" in result_df.columns :
            result_df= result_df.drop(col("data_type_test"))
            
        if "Duplicate_indicator" in result_df.columns :
            result_df  =result_df.drop(col("Duplicate_indicator"))
                
        return result_df
    
    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def __get_col_list(src_df:DataFrame)-> List :
    fields_list= [x for x in src_df.columns]
    return fields_list


def __get_lower_case(values:list)-> List :
    fields_list= [x.lower() for x in values]
    return fields_list
