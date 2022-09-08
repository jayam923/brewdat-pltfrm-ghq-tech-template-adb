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
        dq.DataQualityChecker(df)
        .check_column_is_not_null(column_name="name")
        .build_df()
    )

    # ASSERT
    assert 1 == result_df.filter("size(__data_quality_issues) > 0").count()
    bad_record = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    good_record = result_df.filter("id == 1").toPandas().to_dict('records')[0]

    assert bad_record['__data_quality_issues'] is not None
    assert "CHECK_NOT_NULL: Column `name` is null" == bad_record['__data_quality_issues'][0]

    assert good_record['__data_quality_issues'] is None


def test_check_column_max_length_new_dataframe():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
        {"id": "3", "name": "123456789"},
    ])

    # ACT
    result_df = (
        dq.DataQualityChecker(df)
        .check_column_max_length(column_name="name", maximum_length=5)
        .build_df()
    )

    # ASSERT
    record1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    assert record1['__data_quality_issues'] is None

    record2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    assert record2['__data_quality_issues'] is None

    record3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    assert record3['__data_quality_issues'] is not None
    assert ["CHECK_MAX_LENGTH: Column `name` has length 9, which is greater than 5"] == record3['__data_quality_issues']


def test_check_multiple_rules():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john", "email": "j@inbev.com"},
        {"id": "2", "name": None,  "email": None},
        {"id": "3", "name": "123456789", "email": "j@inbev.com"},
        {"id": "4", "name": "123456789", "email": None},
    ])

    # ACT
    result_df = (
        dq.DataQualityChecker(df=df)
        .check_column_max_length(column_name="name", maximum_length=5)
        .check_column_is_not_null(column_name="email")
        .build_df()
    )

    # result_df.explain("extended")

    # ASSERT
    record1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    assert record1['__data_quality_issues'] is None

    record2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    assert record2['__data_quality_issues'] is not None
    assert ["CHECK_NOT_NULL: Column `email` is null"] == record2['__data_quality_issues']

    record3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    assert record3['__data_quality_issues'] is not None
    assert ["CHECK_MAX_LENGTH: Column `name` has length 9, which is greater than 5"] == record3['__data_quality_issues']

    record4 = result_df.filter("id == 4").toPandas().to_dict('records')[0]
    assert record4['__data_quality_issues'] is not None
    assert ["CHECK_MAX_LENGTH: Column `name` has length 9, which is greater than 5", "CHECK_NOT_NULL: Column `email` is null"] == record4['__data_quality_issues']

