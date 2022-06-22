from test.spark_test import spark

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from brewdat.data_engineering.transform_utils import clean_column_names, flatten_struct_columns


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


def test_flatten_struct_columns_no_struct_columns():
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
    result_df = flatten_struct_columns(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_struct_columns():
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
    result_df = flatten_struct_columns(dbutils=None, df=df)

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema


def test_flatten_struct_columns_except_for():
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
            StructField('contact', StructType([
                StructField('email', StringType(), True),
                StructField('phone', StringType(), True)
            ]), True),
            StructField('address__city', StringType(), True),
            StructField('address__country', StringType(), True)
        ]
    )

    # ACT
    result_df = flatten_struct_columns(dbutils=None, df=df, except_for=["contact"])

    # ASSERT
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema

def test_flatten_struct_columns_recursive():
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
    result_df = flatten_struct_columns(dbutils=None, df=df, recursive=True)

    # ASSERT
    result_df.printSchema()
    assert 1 == result_df.count()
    assert expected_schema == result_df.schema

#TESTE RECURSIVO
#TESTE COM ARRAY
#TESTE RECURSIVO COM ARRAY
#TESTE RECURSIVO COM EXCEPT
