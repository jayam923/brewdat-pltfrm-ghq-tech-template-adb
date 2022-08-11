from delta.tables import DeltaTable

from pyspark.sql.types import *

from test.spark_test import spark
from brewdat.data_engineering.write_utils import LoadType, write_delta_table, RunStatus, BadRecordsHandlingMode


schema_name = "test_schema"


def test_append_upsert_load_count(tmpdir):
    df1 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    df2 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000001", },
        {"id": "222", "phone_number": "00000000001", },
    ])

    df3 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    location = f"file://{tmpdir}/test_append_upsert_load_count"
    table_name = "test_append_upsert_load_count"

    print("############# ROUND 1")
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    assert 1 == result1.num_records_loaded
    assert 2 == result2.num_records_loaded
    assert 1 == result3.num_records_loaded


def test_append_upsert_with_nulls_load_count(tmpdir):
    df1 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    df2 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000001", },
        {"id": None, "phone_number": "00000000001", },
    ])

    df3 = spark.createDataFrame([
        {"id": "222", "phone_number": "00000000000", },
        {"id": None, "phone_number": "00000000000", },
    ])

    location = f"file://{tmpdir}/test_append_upsert_with_nulls_load_count"
    table_name = "test_append_upsert_with_nulls_load_count"

    print("############# ROUND 1")
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    assert 1 == result1.num_records_loaded
    assert 2 == result2.num_records_loaded
    assert 2 == result3.num_records_loaded


def test_append_append_new_load_count(tmpdir):
    df1 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    df2 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000001", },
        {"id": "222", "phone_number": "00000000001", },
    ])

    df3 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    location = f"file://{tmpdir}/test_append_append_new_load_count"
    table_name = "test_append_append_new_load_count"

    print("############# ROUND 1")
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    assert 1 == result1.num_records_loaded
    assert 1 == result2.num_records_loaded
    assert 0 == result3.num_records_loaded


def test_type2_scd_load_count(tmpdir):
    df1 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", },
    ])

    df2 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000001", },
        {"id": "222", "phone_number": "00000000001", },
    ])

    df3 = spark.createDataFrame([
        {"id": "333", "phone_number": "00000000000", },
    ])

    location = f"file://{tmpdir}/test_type2_scd_load_count"
    table_name = "test_type2_scd_load_count"

    print("############# ROUND 1")
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )

    assert 1 == result1.num_records_loaded
    print(vars(result2))
    assert 3 == result2.num_records_loaded
    assert 1 == result3.num_records_loaded


def test_write_bad_records_write_to_error_location_mode(tmpdir):
    # ARRANGE
    df_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('phone_number', StringType(), True),
            StructField('__data_quality_issues', ArrayType(StructType()), True),
        ]
    )

    df1 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": []},
    ])
    df2 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
    ])
    df3 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000", "__data_quality_issues": []},
    ], schema=df_schema)
    df4 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
    ])
    location = f"file://{tmpdir}/test_write_bad_records_write_to_error_location_mode"
    table_name = "test_write_bad_records_write_to_error_location_mode"

    # ACT
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_records_handling_mode=BadRecordsHandlingMode.WRITE_TO_ERROR_LOCATION,
    )

    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_records_handling_mode=BadRecordsHandlingMode.WRITE_TO_ERROR_LOCATION,
    )

    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_records_handling_mode=BadRecordsHandlingMode.WRITE_TO_ERROR_LOCATION,
    )

    result4 = write_delta_table(
        spark=spark,
        df=df4,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_records_handling_mode=BadRecordsHandlingMode.WRITE_TO_ERROR_LOCATION,
    )

    # ASSERT
    print(vars(result1))
    assert RunStatus.SUCCEEDED == result1.status
    assert 3 == result1.num_records_read
    assert 2 == result1.num_records_loaded
    assert 1 == result1.num_records_errored_out

    assert RunStatus.SUCCEEDED == result2.status
    assert 1 == result2.num_records_read
    assert 1 == result2.num_records_loaded
    assert 0 == result2.num_records_errored_out

    assert RunStatus.SUCCEEDED == result3.status
    assert 1 == result3.num_records_read
    assert 1 == result3.num_records_loaded
    assert 0 == result3.num_records_errored_out

    assert RunStatus.SUCCEEDED == result4.status
    assert 1 == result4.num_records_read
    assert 0 == result4.num_records_loaded
    assert 1 == result4.num_records_errored_out

    result_df = spark.table(result3.target_object)
    assert "__data_quality_issues" not in result_df.columns
    assert 4 == result_df.count()
    