from test.spark_test import spark

from brewdat.data_engineering import data_quality_utils as dq


def test_null_check_new_dataframe():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
    ])
    df = dq.create_required_columns_for_dq_check(df)

    # ACT
    result_df = dq.null_check(df=df, field_name="name", dbutils=None)

    # ASSERT
    assert 1 == result_df.filter("__bad_record == true").count()
    bad_record = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    good_record = result_df.filter("id == 1").toPandas().to_dict('records')[0]

    assert bad_record['__bad_record']
    assert bad_record['__data_quality_issues']
    assert "name: record contain null value." == bad_record['__data_quality_issues'][0]

    assert not good_record['__bad_record']
    assert not good_record['__data_quality_issues']


def test_null_check_dataframe_with_previous_check():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john", "__bad_record": "True", "__data_quality_issues": ["previous error"]},
        {"id": "2", "name": None, "__bad_record": "False", "__data_quality_issues": []},
        {"id": "3", "name": None, "__bad_record": "True", "__data_quality_issues": ["previous error"]},
        {"id": "4", "name": "mary", "__bad_record": "False", "__data_quality_issues": []},
    ])

    # ACT
    result_df = dq.null_check(df=df, field_name="name", dbutils=None)

    # ASSERT
    assert 3 == result_df.filter("__bad_record == true").count()
    bad_record_1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    bad_record_2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    bad_record_3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    good_record_4 = result_df.filter("id == 4").toPandas().to_dict('records')[0]

    assert bad_record_1['__bad_record']
    assert bad_record_1['__data_quality_issues']
    assert "previous error" == bad_record_1['__data_quality_issues'][0]

    assert bad_record_2['__bad_record']
    assert bad_record_2['__data_quality_issues']
    assert "name: record contain null value." == bad_record_2['__data_quality_issues'][0]

    assert bad_record_3['__bad_record']
    assert ["previous error", "name: record contain null value."] == bad_record_3['__data_quality_issues']

    assert not good_record_4['__bad_record']
    assert not good_record_4['__data_quality_issues']


def test_max_length_new_dataframe():
    # ARRANGE
    df = spark.createDataFrame([
        {"id": "1", "name": "john"},
        {"id": "2", "name": None},
        {"id": "3", "name": "123456789"},
    ])
    df = dq.create_required_columns_for_dq_check(df)

    # ACT
    result_df = dq.max_length(df=df, field_name="name", maximum_length=5, dbutils=None)

    # ASSERT
    record1 = result_df.filter("id == 1").toPandas().to_dict('records')[0]
    assert not record1['__bad_record']
    assert not record1['__data_quality_issues']

    record2 = result_df.filter("id == 2").toPandas().to_dict('records')[0]
    assert not record2['__bad_record']
    assert not record2['__data_quality_issues']

    record3 = result_df.filter("id == 3").toPandas().to_dict('records')[0]
    assert record3['__bad_record']
    assert record3['__data_quality_issues']
    assert ["name: max length of 5 exceeded."] == record3['__data_quality_issues']
