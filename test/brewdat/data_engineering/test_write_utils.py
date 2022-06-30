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
    
    
    
def test_location_already_exists(tmpdir):
    df = spark.createDataFrame([
    {
        "phone_number": "00000000000",
        "name": "my name",
        "address": "my address"
    }
    ])
    location = f"{tmpdir}/test_location_exists"
    schema_name = "test_schema"
    table_name = "test_location_exists"
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    
    new_location = f"{tmpdir}/test_location_exists_new_location"
    
    result_1 = write_delta_table(
        spark=spark,
        df=df,
        location=new_location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    
    
    assert  result_1.status == RunStatus.FAILED
    assert  result_1.error_message == f"Metastore table already exists with a different location. To drop the existing table, use: DROP TABLE `{schema_name}`.`{table_name}`"

    