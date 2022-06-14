from test.spark_test import spark

from brewdat.data_engineering.transform import clean_column_names


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
    result_df = clean_column_names(df)

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
    result_df = clean_column_names(df, except_for=["address 1"])

    # ASSERT
    assert "phone_number" in result_df.columns
    assert "name_Complete_" in result_df.columns
    assert "address 1" in result_df.columns
