from test.spark_test import spark
from brewdat.data_engineering import *
from brewdat.data_engineering.write_utils import LoadType, write_delta_table,SchemaEvolutionMode
from brewdat.data_engineering.common_utils import RunStatus
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import pandas as pd

def test_write_delta_table_append_all(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    location = f"{tmpdir}/test_write_delta_table_append_all"
    schema_name = "test_schema"
    table_name = "test_write_delta_table_append_all"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 1 == result_df.count()
    result_df.show()
    
def test_write_scd_type_2(location,schema_name,table_name):
    print("Testing Type 2 SCD")
    print("""Test performed are
    1. Inserts
    2. Update
    3. Insert + Update
    4. Insert + Update + New Column
    """)
    key_columns = ["ID"]
    
    print("Inserting one record")
    df = spark.createDataFrame([{"ID":1,"Name":"Srivatsan","Age":27}])
    results = write_delta_table(
    spark=spark,
    df=df,
    location=location,
    schema_name=schema_name,
    table_name=table_name,
    load_type=LoadType.TYPE_2_SCD,
    key_columns=key_columns,
    schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )

    print("Updating same record")
    df = spark.createDataFrame([{"ID":1,"Name":"Srivatsan","Age":28}])
    results = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.TYPE_2_SCD,
        key_columns=key_columns,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )

    print("Inserting new record and adding the same record from prev run again(it should not be updated)")
    df = spark.createDataFrame([{"ID":1,"Name":"Srivatsan","Age":28},{"ID":2,"Name":"Messi","Age":33}])
    results = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.TYPE_2_SCD,
        key_columns=key_columns,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )

    print("Inserting one record, updating another record, adding extra column to check schema evolution")
    df = spark.createDataFrame([{"ID":2,"Name":"Lionel Messi","Age":35,"club":"psg"},{"ID":3,"Name":"Iniesta","Age":38,"club":'fcb'}])
    results = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.TYPE_2_SCD,
        key_columns=key_columns,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )

    
    print("Actual data")
    curr_date = datetime.strftime(datetime.today(),"%Y-%m-%d")
    d = [[1, 'srivatsan', 27, False, curr_date, curr_date, None],
    [1, 'srivatsan', 28, True, curr_date, None, None],
    [2, 'messi', 33, False, curr_date, curr_date, None],
    [3, 'iniesta', 38, True, curr_date, None, 'fcb'],
    [2, 'lionel messi', 35, True, curr_date, curr_date, 'psg']]
    col = ["id","name","age","__active_flag","__start_date","__end_date","club"]
    display(pd.DataFrame(d,columns=col).sort_values(by=["id","__active_flag"]))
    print("Table in DB")
    display(spark.table(f"{schema_name}.{table_name}").sort("id","__active_flag"))

    assert spark.sql(f"select count(*) from {schema_name}.{table_name}").collect()[0][0] == 5
    assert spark.sql(f"select count(*) from {schema_name}.{table_name} where __active_flag = True").collect()[0][0] == 3
    assert spark.sql(f"select count(*) from {schema_name}.{table_name} where __active_flag = False").collect()[0][0] == 2
    assert spark.sql(f"select club from {schema_name}.{table_name} where lower(name) = 'iniesta'").collect()[0][0] == 'fcb'
    assert spark.sql(f"select count(*) from {schema_name}.{table_name} where __active_flag = False and __end_date is not null").collect()[0][0] == 2
    
def cleanup(dbutils,path, db_name, tb_name):
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{tb_name}")
    print("Dropped table")
    dbutils.fs.rm(path,recurse=True)

    