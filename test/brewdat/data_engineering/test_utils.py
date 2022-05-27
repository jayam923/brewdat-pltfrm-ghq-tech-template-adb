import pytest

from test.spark_test import spark

from brewdat.data_engineering.utils import BrewDatLibrary

brewdat_library = BrewDatLibrary(spark=spark, dbutils=None)

def test_clean_column_names():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": '00000000000'
            , "name (Complete)": 'my name'
            , "address 1": "my address"
         }
    ])

    #ACT
    result_df = brewdat_library.clean_column_names(df)

    #ASSERT
    assert 'phone_number' in result_df.columns
    assert 'name_Complete_' in result_df.columns
    assert 'address_1' in result_df.columns

def test_clean_column_names_except_for():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": '00000000000'
            , "name (Complete)": 'my name'
            , "address 1": "my address"
         }
    ])

    #ACT
    result_df = brewdat_library.clean_column_names(df, except_for=['address 1'])

    #ASSERT
    assert 'phone_number' in result_df.columns
    assert 'name_Complete_' in result_df.columns
    assert 'address 1' in result_df.columns

def test_read_raw_dataframe_csv():
    #ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/my_data_extraction.csv"

    #ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.CSV
        , location=file_location)

    #ASSERT
    df.show()

    assert 2 == df.count()
    assert 'name' in df.columns
    assert 'address' in df.columns
    assert 'phone' in df.columns
