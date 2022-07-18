import re
import ast
import pandas as pd
import pyspark
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StringType,StructField
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col, count, lit, length
from pyspark.sql.types import IntegerType,DecimalType,ByteType,StringType,LongType,BooleanType,DoubleType,FloatType
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.context import SparkContext
from datetime import datetime as dt

def data_type_check(i:int,logic_df,src_df:DataFrame,file_name,reject_Path, fields_list)->DataFrame:
    """Checks the field datatype for field present at ith position of the
    validation dataframe.

    Parameters
    ----------
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame: 
        Returns datatype enforced column for field present in ith position.
    """
    
    df_final=None
    if logic_df.loc[i,'DataType']== "byte" :
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(ByteType()))

    elif logic_df.loc[i,'DataType']== "Integer" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(IntegerType()))

    elif logic_df.loc[i,'DataType']== "string" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(StringType()))

    elif logic_df.loc[i,'DataType']== "bigint" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(LongType()))

    elif logic_df.loc[i,'DataType']== "bool" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(BooleanType()))

    elif logic_df.loc[i,'DataType']== "decimal" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(DecimalType(12,5)))

    elif logic_df.loc[i,'DataType']== "float" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(FloatType()))

    elif logic_df.loc[i,'DataType']== "double" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',col(logic_df.loc[i,'FieldName'])\
            .cast(DoubleType()))

    if df_final is None:
        return None,src_df
    else:
        
        df_error=df_final.filter((col(f'{logic_df.loc[i,"FieldName"]}_type').isNull()) & (col(logic_df.loc[i,"FieldName"]).isNotNull()))
        src_df=df_final.subtract(df_error).drop(col(f'{logic_df.loc[i,"FieldName"]}')).withColumnRenamed(f'{logic_df.loc[i,"FieldName"]}_type',f'{logic_df.loc[i,"FieldName"]}')
        src_df = src_df.select(fields_list)
        df_error=df_error.drop(col(f'{logic_df.loc[i,"FieldName"]}_type')).withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Data type mismatch')).withColumn('CreatedDatetime',lit(dt.now()))

        return df_error,src_df


def null_check(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    
    """Helps to validate null values in the column

    Parameters
    ----------
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        Pyspark Dataframe with src df and final df.
    """


    df_final = src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}').isNull())
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df= src_df.filter((col(f'{logic_df.loc[i,"FieldName"]}').isNotNull()))
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Mandatory column has null values.')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df


def max_length(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    
    """Checks the field column length against the min and max values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        Returns the dataframe where values are higher than  max value
        
    """
    print(logic_df.loc[i,'Maximum_Length'])
    df_final=src_df.filter(length(logic_df.loc[i]['FieldName'])>logic_df.loc[i,'Maximum_Length'])
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df=src_df.subtract(df_final)
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is longer than Max length.')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df


def min_length(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column length against the min and max values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        Returns the Dataframe where we have values lower than  minimum value
    """
    print(col(logic_df.loc[i,"FieldName"]))
    print(logic_df.loc[i,'Minimum_Length'])
    df_final=src_df.filter(length(col(logic_df.loc[i,"FieldName"]))<logic_df.loc[i,'Minimum_Length'])
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df=src_df.subtract(df_final)
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is lesser than Min length.')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df
    
    
def range_value(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against between min and max values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        Returns the Dataframe where we have values lower than  minimum value
    """
    
    df_range=src_df.filter(col(logic_df.loc[i,"FieldName"]).between(logic_df.loc[i,'Minimum_value'], logic_df.loc[i,'Maximum_value']))
    if df_range.count() == 0:
        return None,src_df
    else:
        df_final=src_df.subtract(df_range)
        src_df = df_range
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is not in between Min and max value.')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df

    
def valid_values(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against valid values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        [DataFrame]: Returns the Dataframe with valid values
    """
    list_values=ast.literal_eval(logic_df.loc[i,"Valid_Values"])
    df_valid=src_df.where(col(logic_df.loc[i,"FieldName"]).isin(list_values))
    if df_valid.count() == 0:
        return None,src_df
    else:
        df_final=src_df.subtract(df_valid)
        src_df=df_valid
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field contain invalid values not matching with given list of values')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df
    
def invalid_values(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against valid values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        [DataFrame]: Returns the Dataframe with valid values
    """
    list_values=ast.literal_eval(logic_df.loc[i,"Invalid_Values"])
    df_invalid=src_df.where(col(logic_df.loc[i,"FieldName"]).isin(list_values) == False)
    if df_invalid.count() == 0:
        return None,src_df
    else:
        df_final=src_df.subtract(df_invalid)
        src_df=df_invalid
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field contain invalid values which is matching with given list of values')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df

def valid_regular_expression(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against valid values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        [DataFrame]: Returns the Dataframe with valid values
    """
    print('checking regular expression')
    exp=logic_df.loc[i,"Valid_Regular_Expression"]
    df_valid=src_df.filter(src_df[logic_df.loc[i,"FieldName"]].rlike(exp))
    if df_valid.count() == 0:
        return None,src_df
    else:
        df_final=src_df.subtract(df_valid)
        src_df=df_valid
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field contain invalid values not matching with regular expression')).withColumn('CreatedDatetime',lit(dt.now()))
        return df_final,src_df



def duplicate_check(col_list,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    
    """Checks the field column values against valid values

    Parameters
    ----------
    src_df (DataFrame): source file
    i (int): 
        iterator for row level data for the pandas file parsing
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df DataFrame needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        [DataFrame]: Returns the Dataframe with valid values
    """
    w = Window.partitionBy(col_list)
    df_final=src_df.select('*', f.count('*').over(w).alias('Duplicate_indicator'))
    if df_final.where(col('Duplicate_indicator')>1):
        src_df=df_final.where('Duplicate_indicator = 1').drop('Duplicate_indicator')
        df_final=df_final.where('Duplicate_indicator > 1').drop('Duplicate_indicator')
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{col_list};<Error>:Columns have Duplicate values.')).withColumn('CreatedDatetime',lit(dt.now()))
        dup_count=df_final.count()
        print(str(dup_count)+ ' records found duplicate')
        return df_final,src_df
    else:
        print('No Duplicate records')
        return None,src_df

    

def run_validation(file_name,src_df,file_path_rules,reject_Path)-> DataFrame:
    
    """Checks the field column values against valid values

    Parameters
    ----------
    
    logic_df : DataFrame
        The logic_df DataFrame to get the input values from user
    src_df : DataFrame
        The src_df file needed to verify
    file_name : str
        The Input file name
    reject_Path : str
        Absolute Delta Lake path for the physical location of this rejected delta records.
        
    Returns
    -------
    DataFrame
        [DataFrame]: Returns the Dataframe with valid values
    """
    
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    schema =StructType(src_df.schema.fields+[StructField('FileName',StringType(),False),StructField('ErrorMessage',StringType(),False),StructField('CreatedDatetime',StringType(),False)])
    error_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    logic_df=file_path_rules.toPandas() 
    logic_df["FieldName"]=logic_df["FieldName"].str.lower()
    for col in src_df.columns:
        src_df = src_df.withColumnRenamed(col,col.lower())
    mandatory_fields_list=[item.lower() for item in logic_df['FieldName'].tolist()]
    fields_list= [x.lower() for x in src_df.columns]
    missing_fields=[]
    #Column presence check
    for i in range(0,len(mandatory_fields_list)):
        if mandatory_fields_list[i] not in fields_list:
            missing_fields.append(mandatory_fields_list[i])
    if len(missing_fields)!=0:
        dtime= dt.now() 
        error=f'The columns {missing_fields} are missing.'
        columns = ['FileName', 'ErrorMessage', 'CreatedDateTime']
        vals = [(file_name, error, dtime)]
        print('column missing',missing_fields)
        raise ValueError('Souce Coulms are not matching with Metadata')
    else:
        #Get PK value as list from config_rules run all the validations
        print('Duplicate check started')
        Pk_col_list=[]
        Pk_col_list=logic_df[logic_df['PK']=='Yes']["FieldName"].tolist()
        #Check Unquie/duplcate records
        if len(Pk_col_list)==0:
            pass
        else:   
            dup_df,src_df=duplicate_check(Pk_col_list,logic_df,src_df,file_name,reject_Path)
            if dup_df is None:
                pass
            else:

                error_df=error_df.union(dup_df)


        for i in range(0,logic_df.shape[0]):
            print(i)
            print(logic_df.loc[i,"FieldName"])
            print(src_df.columns)
            if logic_df.loc[i,"FieldName"] in src_df.columns:
                print(logic_df.loc[i,"FieldName"])
                ## Mandatory Checks whether null values present or not
                if logic_df.loc[i,'IsNull'] == "Yes":
                    print('null_check started')
                    df1,src_df = null_check(i,logic_df,src_df,file_name,reject_Path)
                    if df1 is None:
                            pass
                    else:
                        error_df=error_df.union(df1.select(error_df.columns))
                    ## DataType checks
                if logic_df.loc[i,'DataType'] !='None':
                    print('data_type_check check started')
                    df2,src_df= data_type_check(i,logic_df,src_df,file_name,reject_Path, fields_list)
                    if df2 is None:
                                pass
                    else:
                        error_df=error_df.union(df2)

                else:
                    print('No Datatype')

                 ## range checks
                if logic_df.loc[i,'Maximum_value'] != 'None':
                    if logic_df.loc[i,'Minimum_value'] != 'None':
                        print('data_range_check check started')
                        df9,src_df= range_value(i,logic_df,src_df,file_name,reject_Path)
                        if df9 is None:
                                    pass
                        else:
                            error_df=error_df.union(df9)

                else:
                    print('No Maximum_value and Minimum_value')

                ##Passing only not null values for remaining checks
                    ## checking for Max values
                if logic_df.loc[i,'Maximum_Length'] != 'None':
                    print('max_length check started')
                    df3,src_df= max_length(i,logic_df,src_df,file_name,reject_Path)

                    if df3 is None:
                        pass
                    else:
                        error_df=error_df.union(df3)
                else:
                    pass

                ## checking for Min values
                if logic_df.loc[i,'Minimum_Length']  != 'None':
                    print('min_length check started')
                    df4,src_df = min_length(i,logic_df,src_df,file_name,reject_Path)
                    if df4 is None:
                        pass
                    else:
                        error_df=error_df.union(df4)
                else:
                    pass

                ## Checking the Valid values field
                if logic_df.loc[i,'Valid_Values'] != 'None':
                    print('valid_values check started')
                    df5,src_df =valid_values(i,logic_df,src_df,file_name,reject_Path)
                    if df5 is None:
                        pass
                    else:
                        error_df=error_df.union(df5)
                else:
                    pass

                ## Checking the Invalid values field
                if logic_df.loc[i,'Invalid_Values'] != 'None':
                    print('Invalid_values check started')
                    df5,src_df =invalid_values(i,logic_df,src_df,file_name,reject_Path)
                    if df5 is None:
                        pass
                    else:
                        error_df=error_df.union(df5)
                else:
                    pass

                ## Valid regular expression check filed
                if logic_df.loc[i,'Valid_Regular_Expression'] != 'None':
                    print('Valid_Regular_Expression check started')
                    df6,src_df =valid_regular_expression(i,logic_df,src_df,file_name,reject_Path)
                    if df6 is None:
                        pass
                    else:
                        error_df=error_df.union(df6)
                else:
                    pass
            else:
                pass

        print('completed')
        return error_df , src_df