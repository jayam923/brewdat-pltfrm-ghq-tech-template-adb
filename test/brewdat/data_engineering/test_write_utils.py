from test.spark_test import spark

from brewdat.data_engineering import write_utils
from brewdat.data_engineering.write_utils import LoadType, write_delta_table, _recreate_check
from brewdat.data_engineering.common_utils import RunStatus


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
    
    
    
def test_recreate_check(schema_name,table_name,location,assert_check):
   
    recreate_flag = _recreate_check(spark,schema_name,table_name,location)
    assert recreate_flag == assert_check
    
    write_delta_table(spark=spark,
    df=spark.table('brz_ghq_tech_adventureworks.customer'),
    location=location,
    schema_name=schema_name,
    table_name=table_name,
    load_type=write_utils.LoadType.OVERWRITE_TABLE,
    partition_columns=["__ref_dt"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS)
    
    
    df= spark.table(f"{schema_name}.{table_name}").limit(10)
    print(df)
    
    assert location == spark.sql(f"DESC DETAIL {schema_name}.{table_name}").select("location").collect()[0][0]
