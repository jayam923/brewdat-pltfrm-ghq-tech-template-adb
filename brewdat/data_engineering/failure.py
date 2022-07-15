def data_type_check(i,logic_df,src_df,file_name,reject_Path):
    """Checks the field datatype for field present at ith position of the
    validation dataframe.

    Args:
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns datatype enforced column for field present in ith position.
    """
    df_final=None
    if logic_df.loc[i,'DataType']== "datetime" :

        df_final =src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.unix_timestamp(F.col("Date_val"),logic_df.loc[i,"In_Date_Format"])\
            .cast(TimestampType()))

    elif logic_df.loc[i,'DataType']== "date" :
        df_final =src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.unix_timestamp(F.col("Date_val"),logic_df.loc[i, "In_Date_Format"])\
            .cast(DateType()))
        
    elif logic_df.loc[i,'DataType']== "byte" :
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(ByteType()))
        
    elif logic_df.loc[i,'DataType']== "Integer" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(IntegerType()))
        
    elif logic_df.loc[i,'DataType']== "string" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
           .cast(StringType()))
        
    elif logic_df.loc[i,'DataType']== "bigint" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(LongType()))
        
    elif logic_df.loc[i,'DataType']== "bool" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(BooleanType()))
        
    elif logic_df.loc[i,'DataType']== "decimal" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(DecimalType(12,5)))
        
    elif logic_df.loc[i,'DataType']== "float" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(FloatType()))
        
    elif logic_df.loc[i,'DataType']== "double" : 
        df_final=src_df.withColumn(f'{logic_df.loc[i,"FieldName"]}_type',F.col(logic_df.loc[i,'FieldName'])\
            .cast(DoubleType()))
        
    if df_final is None:
        return None,src_df
    else:
        #df_final.filter((col(f'{logic_df.loc[i,"FieldName"]}_type').isNull()) & (col(logic_df.loc[i,"FieldName"].isNotNull()))).show()
        df_error=df_final.filter((col(f'{logic_df.loc[i,"FieldName"]}_type').isNull()) & (col(logic_df.loc[i,"FieldName"]).isNotNull()))
        df_final=df_final.subtract(df_error).drop(F.col(logic_df.loc[i,"FieldName"]))\
                          .withColumnRenamed(f'{logic_df.loc[i,"FieldName"]}_type',logic_df.loc[i,"FieldName"])
        df_error=df_error.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Data type mismatch')).withColumn('CreatedDatetime',lit(dt.now()))
        df_error.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        df_final.show()
        
        return None,df_final


def mandatory_field_check(i,logic_df,src_df,file_name,reject_Path):
    """Helps validate if all mandatory fields are available in type converted File

    Args:
        src_df (DataFrame): flatfile with type enforced columns
        i (int): iterator for row level data for the pandas file parsing

    Returns:
        DataFrame: data failing validation
    """
    
    
    df_final = src_df.where(col(f'{logic_df.loc[i,"FieldName"]}').isNull())
    #null_df=null_df.union(df_final)
    if df_final.count() == 0:
        return None,src_df
    else:
        src_df= src_df.filter((F.col(f'{logic_df.loc[i,"FieldName"]}').isNotNull()))
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Mandatory column has null values.')).withColumn('CreatedDatetime',lit(dt.now()))
        df_final.write.format("delta").option("mergeSchema",True).mode('overwrite').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df


def max_value(i,logic_df,src_df,file_name,reject_Path):
    """Checks the field column length against the min and max values

    Args:
        src_df ([Dataframe]): flatfile with type enforced columns
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the dataframe where values are higher than  max value
    """
    print(logic_df.loc[i,'Maximum_Length'])
    if logic_df.loc[i,'Maximum_Length'] is not None:
        #checking_df=src_df.withColumn("length",length(f'{logic_df.loc[i,"FieldName"]}_type'))
        df_final=src_df.filter(length(logic_df.loc[i]['FieldName'])>logic_df.loc[i,'Minimum_Length'])
        if df_final.count() == 0:
            return None,src_df
        else:
            src_df=df_final.subtract(df_final)
            df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is longer than Max value.')).withColumn('CreatedDatetime',lit(dt.now()))
            df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
            #df_final=spark.createDataFrame([(file_name,str(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:Mandatory column has null values.'),str(dt.now()))], schema)
            return df_final,src_df


def min_value(i,logic_df,src_df,file_name,reject_Path):
    """Checks the field column length against the min and max values

    Args:
        src_df ([Dataframe]): The field column converted to specific datatype
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the Dataframe where we have values lower than  minimum value
    """
    if logic_df.loc[i,'Minimum_Length'] is not None:
        #checking_df=src_df.withColumn("length",length(logic_df.loc[i]['FieldName']))
        df_final=src_df.filter(length(logic_df.loc[i]['FieldName'])<logic_df.loc[i,'Minimum_Length'])
        df_final.show()
        if df_final.count() == 0:
            return None,src_df
        else:
            src_df=src_df.subtract(df_final)
            df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is lesser than Min value.')).withColumn('CreatedDatetime',lit(dt.now()))
            df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
            return df_final,src_df
            

def date_format(i,logic_df,src_df,file_name,reject_Path):
    """Checks the Date format of the field column

    Args:
        src_df ([dataframe]): The field column converted to specific datatype
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the dataframe which contains errors
    """
    if logic_df.loc[i,'Date_Format']== "DD/MM/YYYY":
        exp= """^\d{1,2}\/\d{1,2}\/\d{4}$"""
        return regex_func(exp,src_df,i,logic_df.loc[i,'Date_Format'])
    elif logic_df.loc[i,'Date_Format']== "YYYY":
        exp= """^[1-9]\d{3}$"""
        return regex_func(exp,src_df,i,logic_df.loc[i,'Date_Format'])
    else:
        return None,src_df


def date_range(i,logic_df,src_df,file_name,reject_Path):
    """Checks the whether the date field is in between the given date range.

    Args:
        src_df ([dataframe]): The field column converted to specific datatype
        i ([int]): iterator being passed through

    Returns:
        [DataFrame]: Returns the dataframe which contains errors
    """
    date_value1 = logic_df.loc[i,'Date_Range'][0:2]
    if date_value1!=">=" or date_value1!="<=":
        date_value2=(logic_df.loc[i,'Date_Range'])[0:1]


    if date_value1== ">=":
        df_final= src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}')<(logic_df.loc[i,'Date_Range'][2:]))
    elif date_value1== "<=":
        df_final= src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}')>(logic_df.loc[i,'Date_Range'][2:]))
    elif date_value2== ">":
        df_final= src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}')<(logic_df.loc[i,'Date_Range'][2:]))
    elif date_value2 == "<":
        df_final= src_df.filter(col(f'{logic_df.loc[i,"FieldName"]}')>(logic_df.loc[i,'Date_Range'][2:]))
    else:
        pass

    if df_final.count() == 0:
        return None,src_df
    else:
        src_df=src_df.subtract(df_final)
            df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{logic_df.loc[i,"FieldName"]};<Error>:The Field is lesser than Min value.')).withColumn('CreatedDatetime',lit(dt.now()))
            df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        return df_final,src_df
        
        
def duplicate_check(col_list,logic_df,src_df,file_name,reject_Path):
    df_final=src_df.join(
    src_df.groupBy(col_list).agg((F.count("*")>1).cast("int").alias("Duplicate_indicator")),
    on=col_list,
    how="inner"
    )
    if df_final.where(F.col('Duplicate_indicator')==1):
        src_df=df_final.where(F.col('Duplicate_indicator')==0).drop(F.col('Duplicate_indicator'))
        df_final=df_final.where(F.col('Duplicate_indicator')==1).drop(F.col('Duplicate_indicator'))
        df_final=df_final.withColumn('FileName',lit(file_name)).withColumn("ErrorMessage",lit(f'<Field>:{col_list};<Error>:Columns have Duplicate values.')).withColumn('CreatedDatetime',lit(dt.now()))
        df_final.write.format("delta").option("mergeSchema",True).mode('append').partitionBy('CreatedDatetime').save(reject_Path)
        #df_final=spark.createDataFrame([(file_name,str(f'<Field>:{col_list};<Error>:Columns have Duplicate values.'),str(dt.now()))], schema)
        df_final=df_final.select('FileName','ErrorMessage','CreatedDatetime')
        dup_count=df_final.count()
        print(str(dup_count)+ ' records found duplicate')
        return df_final,src_df
    else:
        print('No Duplicate records')
        return None,src_df
    

