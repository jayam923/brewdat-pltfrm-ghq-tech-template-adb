import pytest

from brewdat.data_engineering.utils import BrewDatLibrary
from test.spark_test import spark

# TODO mock dbutils
brewdat_library = BrewDatLibrary(spark=spark, dbutils=None)


def test_clean_column_names():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": '00000000000',
            "name (Complete)": 'my name',
            "address 1": "my address"
         }
    ])

    # ACT
    result_df = brewdat_library.clean_column_names(df)

    # ASSERT
    assert 'phone_number' in result_df.columns
    assert 'name_Complete_' in result_df.columns
    assert 'address_1' in result_df.columns


def test_clean_column_names_except_for():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": '00000000000',
            "name (Complete)": 'my name',
            "address 1": "my address",
         }
    ])

    # ACT
    result_df = brewdat_library.clean_column_names(df,
                                                   except_for=['address 1'])

    # ASSERT
    assert 'phone_number' in result_df.columns
    assert 'name_Complete_' in result_df.columns
    assert 'address 1' in result_df.columns


def test_read_raw_dataframe_csv():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/my_data_extraction.csv"

    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.CSV
        , location=file_location)

    # ASSERT
    df.show()

    assert 2 == df.count()
    assert 'name' in df.columns
    assert 'address' in df.columns
    assert 'phone' in df.columns


def test_write_delta_table_append_all(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": '00000000000',
            "name": 'my name',
            "address": "my address"
        }
    ])
    location = f'{tmpdir}/test_write_delta_table_append_all'
    schema_name = "test_schema"
    table_name = "test_write_delta_table_append_all"

    # ACT
    result = brewdat_library.write_delta_table(
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=brewdat_library.LoadType.APPEND_ALL,
    )
    print(result)

    # ASSERT
    assert result['status'] == brewdat_library.RunStatus.SUCCEEDED
    result_df = spark.table(result['target_object'])
    assert 1 == result_df.count()
    result_df.show()
