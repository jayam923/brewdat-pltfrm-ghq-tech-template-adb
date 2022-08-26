import pytest

from test.spark_test import spark
from datetime import datetime
from pyspark.sql.types import BooleanType, IntegerType, MapType, StructType, StructField, StringType, ArrayType
from brewdat.data_engineering.transform_utils import *


def test_clean_column_names():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
            "address##2": "my address",
            "(address 3)": "my address",
            "__ingestion_date": "2022-07-19"
         }
    ])

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert "phone_number" in result_df.columns
    assert "name_Complete" in result_df.columns
    assert "address_1" in result_df.columns
    assert "address_2" in result_df.columns
    assert "address_3" in result_df.columns
    assert "__ingestion_date" in result_df.columns


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
    assert "name_Complete" in result_df.columns
    assert "address 1" in result_df.columns


def test_clean_column_names_struct():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('ws:name', StringType(), True),
            StructField('ws:surname', StringType(), True),
            StructField('ws:address', StructType([
                StructField('ws:city', StringType(), True),
                StructField('ws:country', StringType(), True),
                StructField('ws:code', IntegerType(), True)
            ]), True)
        ]
    )
    df = spark.createDataFrame([
        {
            "ws:name": "john",
            "ws:surname": "doe",
            "ws:address": {
                "ws:city": "new york",
                "ws:country": "us",
                "ws:code": 100
            },
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('ws_name', StringType(), True),
            StructField('ws_surname', StringType(), True),
            StructField('ws_address', StructType([
                StructField('ws_city', StringType(), True),
                StructField('ws_country', StringType(), True),
                StructField('ws_code', IntegerType(), True)
            ]), True)
        ]
    )

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_clean_column_names_struct_nested_struct():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('ws:name', StringType(), True),
            StructField('ws:surname', StringType(), True),
            StructField('ws:address', StructType([
                StructField('ws:city', StringType(), True),
                StructField('ws:country', StructType([
                    StructField('ws:name', StringType(), True),
                    StructField('ws:code', StringType(), True)
                ]), True)
            ]), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "ws:name": "john",
            "ws:surname": "doe",
            "ws:address": {
                "ws:city": "new york",
                "ws:country": {
                    "ws:name": "United States",
                    "ws:code": "us"
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('ws_name', StringType(), True),
            StructField('ws_surname', StringType(), True),
            StructField('ws_address', StructType([
                StructField('ws_city', StringType(), True),
                StructField('ws_country', StructType([
                    StructField('ws_name', StringType(), True),
                    StructField('ws_code', StringType(), True)
                ]), True)
            ]), True),
        ]
    )

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_clean_column_names_array_of_struct():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('ws:name', StringType(), True),
            StructField('ws:surname', StringType(), True),
            StructField('ws:addresses', ArrayType(StructType([
                StructField('ws:city', StringType(), True),
                StructField('ws:country', StringType(), True)
            ]), True), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "ws:name": "john",
            "ws:surname": "doe",
            "ws:addresses": [
                {
                    "ws:city": "new york",
                    "ws:country": "us"
                },
                {
                    "ws:city": "london",
                    "ws:country": "uk"
                }
            ]
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('ws_name', StringType(), True),
            StructField('ws_surname', StringType(), True),
            StructField('ws_addresses', ArrayType(StructType([
                StructField('ws_city', StringType(), True),
                StructField('ws_country', StringType(), True)
            ]), True), True),
        ]
    )

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_clean_column_names_map_nested_struct():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('ws:name', StringType(), True),
            StructField('ws:surname', StringType(), True),
            StructField('ws:address', MapType(
                StringType(),
                StructType([
                    StructField('ws:name', StringType(), True),
                    StructField('ws:code', StringType(), True)
                ]), True)
            )
        ]
    )
    df = spark.createDataFrame([
        {
            "ws:name": "john",
            "ws:surname": "doe",
            "ws:address": {
                "country": {
                    "ws:name": "United States",
                    "ws:code": "us"
                }
            }
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('ws_name', StringType(), True),
            StructField('ws_surname', StringType(), True),
            StructField('ws_address', MapType(
                StringType(),
                StructType([
                    StructField('ws_name', StringType(), True),
                    StructField('ws_code', StringType(), True)
                ]), True)
            )
        ]
    )

    # ACT
    result_df = clean_column_names(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


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


def test_flatten_dataframe_map_type():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "address": {
                "city": "new york",
                "country": "us"
            },
            "contact": {
                "email": "johndoe@ab-inbev.com",
                "phone": "9999999"
            },
            "name": "john",
            "surname": "doe",
        }
    ])
    df.printSchema()

    expected_schema = StructType(
        [
            StructField('address__city', StringType(), True),
            StructField('address__country', StringType(), True),
            StructField('contact__email', StringType(), True),
            StructField('contact__phone', StringType(), True),
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)
    result_df.show()

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_array_explode_disabled():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('addresses', ArrayType(StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "addresses": [
                {
                    "city": "new york",
                    "country": "us"
                },
                {
                    "city": "london",
                    "country": "uk"
                }
            ]
        }], schema=original_schema)

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df, explode_arrays=False)

    # ASSERT
    assert 1 == result_df.count()
    assert original_schema == result_df.schema


def test_flatten_dataframe_array():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('countries', ArrayType(StringType()), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "countries": ["us", "uk"]
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('countries', StringType(), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)

    # ASSERT
    assert 2 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_array_of_structs():
    # ARRANGE
    original_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('addresses', ArrayType(StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True), True),
        ]
    )
    df = spark.createDataFrame([
        {
            "name": "john",
            "surname": "doe",
            "addresses": [
                {
                    "city": "new york",
                    "country": "us"
                },
                {
                    "city": "london",
                    "country": "uk"
                }
            ]
        }], schema=original_schema)

    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('surname', StringType(), True),
            StructField('addresses__city', StringType(), True),
            StructField('addresses__country', StringType(), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)

    # ASSERT
    assert 2 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_dataframe_misc_data_types():

    # ARRANGE
    data = [
        (1, "Alice", ["manager", "user"],
         {"age": 35, "hobbies": ["reading", "traveling"], "addresses": [{"street": "Rodeo Drive", "number": 123}]},
         {"sleepy": False}),
        (2, "Bob", ["user"], {"age": 28}, {"sleepy": True, "hungry": False}),
        (3, "Charlie", [], {"age": 23, "hobbies": ["music", "games"], "lotto_numbers": [4, 8, 15, 16, 23, 42]}, {}),
    ]
    schema = """
        id INT,
        name STRING,
        roles ARRAY<STRING>,
        extra_info STRUCT<age INT, hobbies ARRAY<STRING>, addresses ARRAY<STRUCT<street STRING, number INT>>, lotto_numbers ARRAY<INT>>,
        state MAP<STRING, BOOLEAN>
        """
    df = spark.createDataFrame(data, schema)

    expected_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('roles', StringType(), True),
            StructField('extra_info__age', IntegerType(), True),
            StructField('extra_info__hobbies', StringType(), True),
            StructField('extra_info__addresses__street', StringType(), True),
            StructField('extra_info__addresses__number', IntegerType(), True),
            StructField('extra_info__lotto_numbers', IntegerType(), True),
            StructField('state__sleepy', BooleanType(), True),
            StructField('state__hungry', BooleanType(), True),
        ]
    )

    # ACT
    result_df = flatten_dataframe(dbutils=None, df=df)

    # ASSERT
    assert 4 == result_df.filter("id = 1").count()
    assert 1 == result_df.filter("id = 2").count()
    assert 12 == result_df.filter("id = 3").count()
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


def test_deduplicate_records():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2019-11-22",
            
        },
         
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2021-11-21",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"], watermark_column = "date")
    #result_df.show()
    # ASSERT
    assert 1 == result_df.count()
    assert 1 == result_df.filter("date = '2022-11-22'").count()



def test_deduplicate_records_same_date():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
            
        },
         
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"], watermark_column = "date")
    #result_df.show()
    # ASSERT
    assert 1 == result_df.count()
    assert 1 == result_df.filter("date = '2022-11-22'").count()


def test_deduplicate_records_null_date():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao ",
            "last name": "abreu",
            
            
        },
         
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"], watermark_column = "date")
    #result_df.show()
    # ASSERT
    assert 1 == result_df.count()
    assert 1 == result_df.filter("date = '2022-11-22'").count()
    

def test_deduplicate_records_null_key():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "last name": "abreu",
            "date": "2022-11-22"
            
        },
         
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao ",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"], watermark_column = "date")
    #result_df.show()
    # ASSERT
    assert 2 == result_df.count()


def test_deduplicate_records_different_keys():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22"
            
        },
        
        {
            "name": "joao",
            "last name": "abreu2",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"], watermark_column = "date")
    #result_df.show()
    # ASSERT
    assert 2 == result_df.count()


def test_deduplicate_records_without_watermark_column():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22"
            
        },
        
        {
            "name": "joao",
            "last name": "abreu2",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df,key_columns=["name","last name"])
    #result_df.show()
    # ASSERT
    assert 2 == result_df.count()


def test_deduplicate_records_without_key_columns():
    # ARRANGE
    df = spark.createDataFrame([
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22"
            
        },
        
        {
            "name": "joao",
            "last name": "abreu2",
            "date": "2022-11-22",
            
        },
        {
            "name": "joao",
            "last name": "abreu",
            "date": "2022-11-22",
        },
    ])
    # ACT
    result_df = deduplicate_records(dbutils=None, df=df, watermark_column = "date")
    # ASSERT
    assert 2 == result_df.count()



    
    
   