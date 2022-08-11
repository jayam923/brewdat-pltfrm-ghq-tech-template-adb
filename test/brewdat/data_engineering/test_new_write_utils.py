from delta.tables import DeltaTable

from test.spark_test import spark
from brewdat.data_engineering.write_utils import LoadType, write_delta_table, SchemaEvolutionMode


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

    DeltaTable.forPath(spark, location).history().show(5, False)

    assert 1 == result1.num_records_loaded
    print(vars(result2))
    assert 3 == result2.num_records_loaded
    assert 1 == result3.num_records_loaded