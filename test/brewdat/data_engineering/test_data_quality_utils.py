from test.spark_test import spark

from brewdat.data_engineering import data_quality_utils as dq


def test_check_column_is_not_null_new_dataframe():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
    ])

    # ACT
    result_df = (
        dq.DataQualityChecker(df=df, dbutils=None)
        .check_column_is_not_null(column_name="name")
        .build_df()
    )

    # ASSERT
    assert 1 == result_df.filter("size(__data_quality_issues) > 0").count()
    bad_record = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    good_record = result_df.filter("id == 1").toPandas().to_dict('records')[0]

    assert bad_record['__data_quality_issues']
    assert "CHECK_NOT_NULL: Column `name` is null" == bad_record['__data_quality_issues'][0]

    assert not good_record['__data_quality_issues']


def test_check_column_is_not_null_dataframe_with_previous_check():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john", "__data_quality_issues": ["previous error"]},
        {"id": "2", "name": None, "__data_quality_issues": []},
        {"id": "3", "name": None, "__data_quality_issues": ["previous error"]},
        {"id": "4", "name": "mary", "__data_quality_issues": []},
    ])

    # ACT
    result_df = (
        dq.DataQualityChecker(df=df, dbutils=None)
        .check_column_is_not_null(column_name="name")
        .build_df()
    )

    # ASSERT
    assert 3 == result_df.filter("size(__data_quality_issues) > 0").count()
    bad_record_1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    bad_record_2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    bad_record_3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    good_record_4 = result_df.filter("id == 4").toPandas().to_dict('records')[0]

    assert "previous error" == bad_record_1['__data_quality_issues'][0]
    assert "CHECK_NOT_NULL: Column `name` is null" == bad_record_2['__data_quality_issues'][0]
    assert ["previous error", "CHECK_NOT_NULL: Column `name` is null"] == bad_record_3['__data_quality_issues'].tolist()
    assert not good_record_4['__data_quality_issues']


def test_check_column_max_length_new_dataframe():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
        {"id": "3", "name": "123456789"},
    ])

    # ACT
    result_df = (
        dq.DataQualityChecker(df=df, dbutils=None)
        .check_column_max_length(column_name="name", maximum_length=5)
        .build_df()
    )

    # ASSERT
    record1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    assert not record1['__data_quality_issues']

    record2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    assert not record2['__data_quality_issues']

    record3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    assert record3['__data_quality_issues']
    assert ["CHECK_MAX_LENGTH: Column `name` has length 9, which is greater than 5"] == record3['__data_quality_issues'].tolist()
