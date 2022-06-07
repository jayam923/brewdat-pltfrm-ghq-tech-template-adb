import pytest

from typing import List
from pyspark.sql.types import StructType, StructField, StringType

from brewdat.data_engineering.utils import BrewDatLibrary
from test.spark_test import spark

# TODO mock dbutils
brewdat_library = BrewDatLibrary(spark=spark, dbutils=None)


def test_read_raw_dataframe_simple_csv():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/simple_csv1.csv"
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
    assert "name" in df.columns
    assert "address" in df.columns
    assert "phone" in df.columns
    assert expected_schema == df.schema


def test_read_raw_dataframe_simple_parquet():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/simple_parquet1.parquet"
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
    assert "name" in df.columns
    assert "address" in df.columns
    assert "phone" in df.columns
    assert expected_schema == df.schema


def test_read_raw_dataframe_simple_orc():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/simple_orc1.orc"
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
    assert "name" in df.columns
    assert "address" in df.columns
    assert "phone" in df.columns
    assert expected_schema == df.schema


def test_read_raw_dataframe_simple_delta():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/simple_delta1"
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
    assert "name" in df.columns
    assert "address" in df.columns
    assert "phone" in df.columns
    assert expected_schema == df.schema

