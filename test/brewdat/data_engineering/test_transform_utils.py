from test.spark_test import spark

from datetime import datetime

from brewdat.data_engineering.transform_utils import clean_column_names, create_or_replace_audit_columns, create_or_replace_business_key_column


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
    
 def test_business_key_column_isnull():
    # ARRANGE
    df = spark.createDataFrame([
         {
            "name": "elvin",
            "last_name": "geroge",
            "address 1": "street1212",
         }
    ])
    # ACT
    result_df = create_or_replace_business_key_column(
        dbutils=None,
        df=df,
        business_key_column_name='business_key_column_name',
        key_columns=[],
        separator="__",
        check_null_values=True,
    )
test_business_key_column_isnull()
