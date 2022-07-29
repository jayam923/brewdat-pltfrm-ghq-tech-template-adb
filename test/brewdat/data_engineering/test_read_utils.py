from test.spark_test import spark

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType

from brewdat.data_engineering.read_utils import read_raw_dataframe, RawFileFormat


def test_read_raw_dataframe_csv_simple(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/csv_simple1.csv"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )

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
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_simple(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_simple1.parquet"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_orc_simple(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/orc_simple1.orc"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.ORC,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_delta_simple(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/delta_simple1"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True),
            StructField('id', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.DELTA,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_array(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_array.parquet"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('ages', ArrayType(StringType(), True), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_struct(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_struct.parquet"):
    # ARRANGE
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
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_deeply_nested_struct(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct.parquet"):
    # ARRANGE
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
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema



def test_read_raw_dataframe_parquet_with_array_of_struct(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_array_of_struct.parquet"):
    # ARRANGE
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
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_deeply_nested_struct_inside_array(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct_inside_array.parquet"):
    # ARRANGE
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
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_parquet_with_deeply_nested_struct_inside_array_do_not_cast_types(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/parquet_with_deeply_nested_struct_inside_array.parquet"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('users', ArrayType(StructType([
                StructField('users_profile', StructType([
                    StructField('personal_data', StructType([
                        StructField('age', IntegerType(), True),
                        StructField('name', StringType(), True)
                    ]), True),
                    StructField('contact_info', StructType([
                        StructField('address', StringType(), True),
                        StructField('phone', StringType(), True)
                    ]), True)
                ]), True),
                StructField('is_new', BooleanType(), True)
            ]), True), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.PARQUET,
        location=file_location,
        cast_all_to_string=False
    )

    # ASSERT
    df.show()
    assert 1 == df.count()
    assert expected_schema == df.schema

    
def test_read_raw_dataframe_xml_simple(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/xml_simple.xml"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('address', StringType(), True),
            StructField('name', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.XML,
        location=file_location,
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_xml_simple_row_tag(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/xml_simple_tag.xml"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('address', StringType(), True),
            StructField('name', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.XML,
        location=file_location,
        xml_row_tag="client"
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_csv_delimiter(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/csv_delimiter.csv"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.CSV,
        location=file_location,
        csv_delimiter=';'
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_csv_has_headers(file_location = "./test/brewdat/data_engineering/support_files/read_raw_dataframe/csv_no_headers.csv"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('phone', StringType(), True)
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.CSV,
        location=file_location,
        csv_delimiter=',',
        csv_has_headers=False
    )

    # ASSERT
    df.show()
    assert 2 == df.count()
    

def test_read_raw_dataframe_excel_simple(file_location="./test/brewdat/data_engineering/support_files/read_raw_dataframe/excel_simple.xlsx"):
    # ARRANGE
    expected_schema = StructType(
        [
            StructField('name', StringType(), False),
            StructField('address', StringType(), False),
        ]
    )

    # ACT
    df = read_raw_dataframe(
        spark=spark,
        dbutils=None,
        file_format=RawFileFormat.EXCEL,
        location=file_location,
        excel_sheet_name="records"
    )

    # ASSERT
    assert 2 == df.count()
    assert expected_schema == df.schema


def test_read_raw_dataframe_avro(file_location="./test/brewdat/data_engineering/support_files/read_raw_dataframe/avro_simple1"):
        # ARRANGE
        expected_schema = StructType(
            [
                StructField('name', StringType(), True),
                StructField('surname', StringType(), True)
            ]
        )

        # ACT
        df = read_raw_dataframe(
            spark=spark,
            dbutils=None,
            file_format=RawFileFormat.AVRO,
            location=file_location,
        )

        # ASSERT
        df.show()
        assert 2 == df.count()
        assert expected_schema == df.schema
