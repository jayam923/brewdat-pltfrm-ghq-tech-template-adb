from test.spark_test import spark

from brewdat.data_engineering.read_utils import read_raw_dataframe, RawFileFormat


def test_read_raw_dataframe_csv():
    # ARRANGE
    file_location = "./test/brewdat/data_engineering/support_files/my_data_extraction.csv"

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.CSV,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert "name" in df.columns
    assert "address" in df.columns
    assert "phone" in df.columns
