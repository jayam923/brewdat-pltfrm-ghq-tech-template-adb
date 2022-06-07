import pytest

from typing import List
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from brewdat.data_engineering.utils import BrewDatLibrary
from test.spark_test import spark

# TODO mock dbutils
brewdat_library = BrewDatLibrary(spark=spark, dbutils=None)


def test_read_raw_dataframe_csv_simple():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/csv_simple1.csv"
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.CSV,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_simple():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_simple1.parquet"
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_orc_simple():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/orc_simple1.orc"
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.ORC,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_delta_simple():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/delta_simple1"
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )

    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.DELTA,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_array():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_array.parquet"
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('ages', ArrayType(StringType(), True), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_struct():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_struct.parquet"
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('users_profile', StructType([
                StructField('age', StringType(), True),
                StructField('name', StringType(), True)
            ]), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_deeply_nested_struct():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct.parquet"
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('users_profile', StructType([
                StructField('personal_data', StructType([
                    StructField('age', StringType(), True),
                    StructField('name', StringType(), True)
                ]), True),
                StructField('contact_info', StructType([
                    StructField('address', StringType(), True),
                    StructField('phone', StringType(), True)
                ]), True)
            ]), True),
            StructField('is_new', StringType(), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema



def test_read_raw_dataframe_parquet_with_array_of_struct():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_array_of_struct.parquet"
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('users', ArrayType(StructType([
                StructField('age', StringType(), True),
                StructField('name', StringType(), True)
            ]), True), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_deeply_nested_struct_inside_array():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct_inside_array.parquet"
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('users', ArrayType(StructType([
                StructField('users_profile', StructType([
                    StructField('personal_data', StructType([
                        StructField('age', StringType(), True),
                        StructField('name', StringType(), True)
                    ]), True),
                    StructField('contact_info', StructType([
                        StructField('address', StringType(), True),
                        StructField('phone', StringType(), True)
                    ]), True)
                ]), True),
                StructField('is_new', StringType(), True)
            ]), True), True)
        ]
    )
    # ACT
    df = brewdat_library.read_raw_dataframe(
        file_format=BrewDatLibrary.RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema
