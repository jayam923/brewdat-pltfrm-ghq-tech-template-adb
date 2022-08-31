from test.spark_test import spark

from brewdat.data_engineering import data_quality_wider_check as dq
from brewdat.data_engineering import data_quality_wider_check2 as dq2


def test_check_nulls(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
        {"id": "2", "name": None},
    ])
    location = f"{tmpdir}/test_check_nulls"
    df.write.format("delta").mode("overwrite").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_nulls(col_name="name", mostly=0.5)
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_compound_column_uniqueness(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "id2": "1"},
        {"id": "1", "id2": "2"},
        {"id": "2", "id2": "2"},
        {"id": "2", "id2": "2"},
    ])
    location = f"{tmpdir}/test_check_compound_column_uniqueness"
    df.write.format("delta").mode("overwrite").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_compound_column_uniqueness(col_list=["id", "id2"], mostly=0.8)
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_row_count(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "id2": "1"},
        {"id": "1", "id2": "2"},
        {"id": "2", "id2": "2"},
        {"id": "2", "id2": "2"},
    ])
    location = f"{tmpdir}/test_check_row_count"
    df.write.format("delta").mode("overwrite").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_row_count(min_value=1, max_value=2)
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_column_sum(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "qtd": 1},
        {"id": "1", "qtd": 1},
        {"id": "2", "qtd": 1},
        {"id": "2", "qtd": 1},
    ])
    location = f"{tmpdir}/test_check_column_sum"
    df.write.format("delta").mode("overwrite").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_column_sum(col_name="qtd", min_value=1, max_value=2)
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_column_uniqueness(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "qtd": "1"},
        {"id": "1", "qtd": "1"},
        {"id": "2", "qtd": "1"},
        {"id": "2", "qtd": "1"},
    ])
    location = f"{tmpdir}/test_check_column_uniqueness"
    df.write.format("delta").mode("overwrite").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_column_uniqueness(col_name="qtd", mostly=1)
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_count_variation_from_previous_version(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {"id": "1", "qtd": "1"},
        {"id": "1", "qtd": "1"},
    ])
    df2 = spark.createDataFrame([
        {"id": "1", "qtd": "1"},
        {"id": "1", "qtd": "1"},
    ])
    location = f"{tmpdir}/test_check_count_variation_from_previous_version"
    df1.write.format("delta").mode("append").save(location)
    df2.write.format("delta").mode("append").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_count_variation_from_previous_version(
                min_value=0,
                max_value=1,
                previous_version=0
            )
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']


def test_check_null_percentage_variation_from_previous_version(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {"id": "1", "qtd": "1"},
        {"id": "1", "qtd": "1"},
    ])
    df2 = spark.createDataFrame([
        {"id": "1", "qtd": "1"},
        {"id": None, "qtd": "1"},
    ])
    location = f"{tmpdir}/test_check_count_variation_from_previous_version"
    df1.write.format("delta").mode("append").save(location)
    df2.write.format("delta").mode("append").save(location)

    # ACT
    result_df = (
        dq2.DataQualityCheck(location=location, dbutils=None, spark=spark)
            .check_null_percentage_variation_from_previous_version(
                col_name="id",
                max_accepted_variation=0.1,
                previous_version=0
            )
            .build()
    )

    # ASSERT
    result_df.show(100, False)

    result = result_df.toPandas().to_dict('records')[0]
    print(result)
    assert not result['passed']

