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

def data_type_check(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field datatype for field present at ith position of the
    validation dataframe.

    Args:
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns datatype enforced column for field present in ith position.
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
        
        df_error=df_final.filter((col(f'{logic_df.loc[i,"FieldName"]}_type').isNull())) #& (col(logic_df.loc[i,"FieldName"]).isNotNull()))
        src_df=df_final.subtract(df_error).drop(col(f'{logic_df.loc[i,"FieldName"]}_type'))                          
        df_error=df_error.drop(col(f'{logic_df.loc[i,"FieldName"]}_type')).withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Data type mismatch')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_error.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)


        return df_error,src_df


def mandatory_field_check(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Helps validate if all mandatory fields are available in type converted File

    Args:
        src_df (DataFrame): source file
        i (int): iterator for row level data for the pandas file parsing

    Returns:
        DataFrame: data failing validation
    """


    df_final = src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}').isNull())
    if df_final.count() == 0:
        print("inside_if_condition_df_final")
        return None,src_df
    else:
        print("inside_else_condition_df_final")
        src_df= src_df.filter((col(f'{logic_df.loc[i,"FieldName"]}').isNotNull()))
        print(src_df)
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Mandatory column has null values.')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        print(df_final)
        return df_final,src_df


def max_value(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column length against the min and max values

    Args:
        src_df ([Dataframe]): source file
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the dataframe where values are higher than  max value
    """
    df_final=src_df.filter(length(logic_df.loc[i]['FieldName'])>logic_df.loc[i,'Minimum_Length'])
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df=df_final.subtract(df_final)
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is longer than Max value.')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df


def min_value(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column length against the min and max values

    Args:
        src_df ([Dataframe]): source file
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the Dataframe where we have values lower than  minimum value
    """
    df_final=src_df.filter(length(col(logic_df.loc[i,"FieldName"]))<logic_df.loc[i,'Minimum_Length'])
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df=src_df.subtract(df_final)
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is lesser than Min value.')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df

def valid_values(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against valid values

    Args:
        i ([int]): iterator being passed through

    Returns:
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
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df

def valid_regular_expression(i:int,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    """Checks the field column values against valid values

    Args:
        i ([int]): iterator being passed through

    Returns:
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
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df


def duplicate_check(col_list,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    df_final=src_df.join(
    src_df.groupBy(col_list).agg((count("*")>1).cast("int").alias("Duplicate_indicator")),
    on=col_list,
    how="inner"
    )
    if df_final.where(col('Duplicate_indicator')==1):
        src_df=df_final.where(col('Duplicate_indicator')==0).drop(col('Duplicate_indicator'))
        df_final=df_final.where(col('Duplicate_indicator')==1).drop(col('Duplicate_indicator'))
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{col_list};<Error>:Columns have Duplicate values.')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        #df_final=spark.createDataFrame([(file_name,str(f'<Field>:{col_list};<Error>:Columns have Duplicate values.'),str(dt.now()))], schema)
        #df_final=df_final.select('FileName','ErrorMessage','CreatedDatetime')
        dup_count=df_final.count()
        print(str(dup_count)+ ' records found duplicate')
        return df_final,src_df
    else:
        print('No Duplicate records')
        return None,src_df

    
def duplicate_check_test(col_list,logic_df,src_df:DataFrame,file_name,reject_Path)->DataFrame:
    w = Window.partitionBy(col_list)
    df_final=src_df.select('*', f.count('*').over(w).alias('Duplicate_indicator'))
    if df_final.where(col('Duplicate_indicator')>1):
        src_df=df_final.where('Duplicate_indicator = 1').drop('Duplicate_indicator')
        df_final=df_final.where('Duplicate_indicator > 1').drop('Duplicate_indicator')
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{col_list};<Error>:Columns have Duplicate values.')).withColumn('CreatedDatetime',lit(dt.now()))
        #df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        #df_final=spark.createDataFrame([(file_name,str(f'<Field>:{col_list};<Error>:Columns have Duplicate values.'),str(dt.now()))], schema)
        #df_final=df_final.select('FileName','ErrorMessage','CreatedDatetime')
        dup_count=df_final.count()
        print(str(dup_count)+ ' records found duplicate')
        return df_final,src_df
    else:
        print('No Duplicate records')
        return None,src_df

    

def run_validation(file_name,src_df,file_path_rules,reject_Path)-> DataFrame:
    # Checking if mandatory fields are present or not.
    #mandatory_fields_df= pd.read_csv(file_path_rules)
    #schema =StructType([StructField('FileName',StringType(),False),StructField('ErrorMessage',StringType(),False),StructField('CreatedDatetime',StringType(),False)])
    #error_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    schema =StructType(src_df.schema.fields+[StructField('FileName',StringType(),False),StructField('ErrorMessage',StringType(),False),StructField('CreatedDatetime',StringType(),False)])
    error_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    logic_df=file_path_rules.toPandas() 
    mandatory_fields_df=logic_df
    listr =mandatory_fields_df['FieldName'].tolist()
    mandatory_fields_list=[item.lower() for item in listr]
    col=src_df.columns
    fields_list= [x.lower() for x in col]
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
        #missing_fields_error_df = spark.createDataFrame(vals, columns)
        #error_df=error_df.union(missing_fields_error_df)
        print('column missing',missing_fields)
        #raise ValueError('Souce Coulms are not matching with Metadata')
    #else:
        #Get PK value as list from config_rules
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
        if logic_df.loc[i,"FieldName"] in src_df.columns:
            ## Mandatory Checks whether null values present or not
            if logic_df.loc[i,'IsMandatory'] == "Yes":
                df1,src_df = mandatory_field_check(i,logic_df,src_df,file_name,reject_Path)
                if df1 is None:
                        pass
                else:
                    error_df=error_df.union(df1.select(error_df.columns))
                ## DataType checks
            if logic_df.loc[i,'DataType'] !='None':
                df2,src_df= data_type_check(i,logic_df,src_df,file_name,reject_Path)
                if df2 is None:
                            pass
                else:
                    error_df=error_df.union(df2)

            else:
                print('No Datatype')

            ##Passing only not null values for remaining checks
                ## checking for Max values
            if logic_df.loc[i,'Maximum_Length'] != 'None':
                df3,src_df= max_value(i,logic_df,src_df,file_name,reject_Path)
                if df3 is None:
                    pass
                else:
                    error_df=error_df.union(df3.select(error_df.columns))
            else:
                pass

            ## checking for Min values
            if logic_df.loc[i,'Minimum_Length']  != 'None':
                df4,src_df = min_value(i,logic_df,src_df,file_name,reject_Path)
                if df4 is None:
                    pass
                else:
                    error_df=error_df.union(df4.select(error_df.columns))
            else:
                pass

            ## Checking the Valid values field
            if logic_df.loc[i,'Valid_Values'] != 'None':
                df5,src_df =valid_values(i,logic_df,src_df,file_name,reject_Path)
                if df5 is None:
                    pass
                else:
                    error_df=error_df.union(df5.select(error_df.columns))
            else:
                pass
            
            ## Valid regular expression check filed
            if logic_df.loc[i,'Valid_Regular_Expression'] != 'None':
                df6,src_df =valid_regular_expression(i,logic_df,src_df,file_name,reject_Path)
                if df6 is None:
                    pass
                else:
                    error_df=error_df.union(df6.select(error_df.columns))
            else:
                pass
        else:
            pass

    print('completed')
    return error_df , src_df