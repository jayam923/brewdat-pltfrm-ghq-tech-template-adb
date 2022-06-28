from test.spark_test import spark

from datetime import datetime

from brewdat.data_engineering.transform_utils import clean_column_names, create_or_replace_audit_columns


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
    df=spark.createdataframe([
         {
            "phone  number": "00000000000",
            "name (Complete)": "my name",
            "address 1": "my address",
         }
    ])
    
    #ACT
    result_df = create_or_replace_business_key_column(dbutils=None, df=df, business_key_column_name='exact location', key_columns=["Hyd","Ameerpet"], separator: "__", check_null_values= True )
    
    # ASSERT
    assert "phone_number" in result_df.columns
    assert "name_Complete_" in result_df.columns
    assert "address 1" in result_df.columns
    assert "business_key_column_name"  in result_df.columns