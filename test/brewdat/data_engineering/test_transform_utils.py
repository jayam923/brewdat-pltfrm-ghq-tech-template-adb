import pytest

from test.spark_test import spark
from datetime import datetime
from brewdat.data_engineering.transform_utils import clean_column_names, create_or_replace_audit_columns, create_or_replace_business_key_column
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from brewdat.data_engineering.transform_utils import clean_column_names, create_or_replace_audit_columns, flatten_dataframe



def test_clean_column_names():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address"
         }
    ])

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert "phone_number" in result_df.columns
    assert "name_Complete_" in result_df.columns
    assert "address_1" in result_df.columns


def test_clean_column_names_except_for():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
         }
    ])

    # ACT
    result_df = clean_column_names(dbutils=None, df=df, except_for=["address 1"])

    # ASSERT
    assert "phone_number" in result_df.columns
    assert "name_Complete_" in result_df.columns
    assert "address 1" in result_df.columns


def test_flatten_dataframe_no_struct_columns():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe"
        }
    ])
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True),
            StructField('contact', StructType([
                StructField('email', StringType(), True),
                StructField('phone', StringType(), True)
            ]), True)
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": "us"
            },
            "contact": {
                "email": "johndoe@ab-inbev.com",
                "phone": "9999999"
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country', StringType(), True),
            StructField('contact__email', StringType(), True),
            StructField('contact__phone', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_custom_separator():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True),
            StructField('contact', StructType([
                StructField('email', StringType(), True),
                StructField('phone', StringType(), True)
            ]), True)
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": "us"
            },
            "contact": {
                "email": "johndoe@ab-inbev.com",
                "phone": "9999999"
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address___city', StringType(), True),
            StructField('address___country', StringType(), True),
            StructField('contact___email', StringType(), True),
            StructField('contact___phone', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, column_name_separator="___")

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_except_for():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True),
            StructField('contact', StructType([
                StructField('email', StringType(), True),
                StructField('phone', StringType(), True)
            ]), True)
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": "us"
            },
            "contact": {
                "email": "johndoe@ab-inbev.com",
                "phone": "9999999"
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country', StringType(), True),
            StructField('contact', StructType([
                StructField('email', StringType(), True),
                StructField('phone', StringType(), True)
            ]), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, except_for=["contact"])

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_recursive():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StructType([
                    StructField('name', StringType(), True),
                    StructField('code', StringType(), True)
                ]), True)
            ]), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": {
                    "name": "United States",
                    "code": "us"
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country__name', StringType(), True),
            StructField('address__country__code', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, recursive=True)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_not_recursive():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StructType([
                    StructField('name', StringType(), True),
                    StructField('code', StringType(), True)
                ]), True)
            ]), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": {
                    "name": "United States",
                    "code": "us"
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country', StructType([
                StructField('name', StringType(), True),
                StructField('code', StringType(), True)
            ]), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, recursive=False)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_recursive_deeply_nested():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StructType([
                    StructField('name', StringType(), True),
                    StructField('reference', StructType([
                        StructField('code', StringType(), True),
                        StructField('abbreviation', StringType(), True),
                    ]), True)
                ]), True)
            ]), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": {
                    "name": "United States",
                    "reference": {
                        "code": "***",
                        "abbreviation": "us"
                    }
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country__name', StringType(), True),
            StructField('address__country__reference__code', StringType(), True),
            StructField('address__country__reference__abbreviation', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, recursive=True)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_recursive_except_for():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StructType([
                    StructField('name', StringType(), True),
                    StructField('code', StringType(), True)
                ]), True)
            ]), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": {
                    "name": "United States",
                    "code": "us"
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country', StructType([
                StructField('name', StringType(), True),
                StructField('code', StringType(), True)
            ]), True)
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, recursive=True, except_for=['address__country'])

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_preserve_columns_order():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StructType([
                    StructField('name', StringType(), True),
                    StructField('reference', StructType([
                        StructField('code', StringType(), True),
                        StructField('abbreviation', StringType(), True),
                    ]), True)
                ]), True)
            ]), True),
            StructField('username', StringType(), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "address": {
                "city": "new york",
                "country": {
                    "name": "United States",
                    "reference": {
                        "code": "***",
                        "abbreviation": "us"
                    }
                }
            },
            "username": "john_doe"
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('address__city', StringType(), True),
            StructField('address__country__name', StringType(), True),
            StructField('address__country__reference__code', StringType(), True),
            StructField('address__country__reference__abbreviation', StringType(), True),
            StructField('username', StringType(), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, recursive=True)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_create_or_replace_audit_columns():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
        }
    ])
    # ACT
    result_df = create_or_replace_audit_columns(dbutils=None, df=df, )
    # ASSERT
    assert "__insert_gmt_ts" in result_df.columns
    assert "__update_gmt_ts" in result_df.columns
    assert result_df.filter("__insert_gmt_ts is null").count() == 0
    assert result_df.filter("__update_gmt_ts is null").count() == 0


def test_create_or_replace_audit_columns_already_exist():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
            "__insert_gmt_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),

        },
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
            "__insert_gmt_ts": None,

        }
    ])
    # ACT
    result_df = create_or_replace_audit_columns(dbutils=None, df=df, )
    # ASSERT
    assert "__insert_gmt_ts" in result_df.columns
    assert "__update_gmt_ts" in result_df.columns
    assert result_df.filter("__insert_gmt_ts is null").count() == 0
    assert result_df.filter("__update_gmt_ts is null").count() == 0


def test_create_or_replace_business_key_column():
    # ARRANGE
    df = spark.createDataFrame([
         {
            "name": "john",
            "last_name": "doe",
            "address 1": "my address",
         }
    ])
    
    # ACT
    result_df = create_or_replace_business_key_column(
        dbutils=None,
        df=df,
        business_key_column_name='business_key_column_name',
        key_columns=["name", "last_name"],
        separator="__",
        check_null_values=True,
    )
    
    # ASSERT
    assert "business_key_column_name" in result_df.columns
    assert 1 == result_df.filter("business_key_column_name = 'john__doe' ").count()


def test_business_key_column_no_key_provided():
    # ARRANGE
    df = spark.createDataFrame([
         {
            "name": "elvin",
            "last_name": "geroge",
            "address 1": "street1212",
         }
    ])
    # ACT
    with pytest.raises(Exception):
        result_df = create_or_replace_business_key_column(
            dbutils=None,
            df=df,
            business_key_column_name='business_key_column_name',
            key_columns=[],
            separator="__",
            check_null_values=True,
        )


def test_business_key_column_keys_are_null():
    # ARRANGE
    df = spark.createDataFrame([
         {
            "name": 'elvin',
            "last_name": "geroge",
            "address 1": "street1212",
         },
        {
            "name": None,
            "last_name": "geroge",
            "address 1": "street1212",
        }
    ])
    # ACT
    with pytest.raises(Exception):
        result_df = create_or_replace_business_key_column(
            dbutils=None,
            df=df,
            business_key_column_name='business_key_column_name',
            key_columns=["name", "last_name"],
            separator="__",
            check_null_values=True,
        )