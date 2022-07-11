from test.spark_test import spark

from pyspark.sql.types import StructType, StructField, StringType

from brewdat.data_engineering.write_utils import LoadType, write_delta_table, SchemaEvolutionMode
from brewdat.data_engineering.common_utils import RunStatus


def test_write_delta_table_append_all(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    location = f"file://{tmpdir}/test_write_delta_table_append_all"
    schema_name = "test_schema"
    table_name = "test_write_delta_table_append_all"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 1 == result_df.count()
    result_df.show()


def test_location_already_exists(tmpdir):
    df = spark.createDataFrame([{
        "phone_number": "00000000000",
        "name": "my name",
        "address": "my address"
    }])
    location = f"{tmpdir}/test_location_exists"
    schema_name = "test_schema"
    table_name = "test_location_exists"
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    print(vars(result))
    
    new_location = f"file://{tmpdir}/test_location_exists_new_location"
    
    result_1 = write_delta_table(
        spark=spark,
        df=df,
        location=new_location,
        schema_name=schema_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    print(vars(result_1))
    
    assert result_1.status == RunStatus.FAILED
    assert result_1.error_message == f"Metastore table already exists with a different location. To drop the existing table, use: DROP TABLE `{schema_name}`.`{table_name}`"


def test_write_scd_type_2_first_write(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_first_write"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_first_write"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 2 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


def test_write_scd_type_2_only_new_ids(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "333",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file:{tmpdir}/test_write_scd_type_2_only_new_ids"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_only_new_ids"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    print(result.error_details)
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 3 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


def test_write_scd_type_2_only_updates(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "222",
            "phone_number": "11111111111",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_only_updates"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_only_updates"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 3 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and __is_active = false "
                                 "and __start_date is not null "
                                 "and __end_date is not null").count()


def test_write_scd_type_2_same_id_same_data(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_same_id_same_data"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_same_id_same_data"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 2 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


def test_write_scd_type_2_updates_and_new_records(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "phone_number": "11111111111",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "333",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_updates_and_new_records"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_updates_and_new_records"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 4 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and __is_active = false "
                                 "and __start_date is not null "
                                 "and __end_date is not null").count()
    assert 1 == result_df.filter("id = '333' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


def test_write_scd_type_2_multiple_keys(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "id2": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "222",
            "id2": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "222",
            "id2": "111",
            "phone_number": "11111111111",
            "name": "my name",
            "address": "my address"
        },
        {
            "id": "111",
            "id2": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_multiple_keys"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_multiple_keys"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id", "id2"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id", "id2"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 4 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and id2 = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '111' "
                                 "and id2 = '222' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and id2 = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()
    assert 1 == result_df.filter("id = '222' "
                                 "and id2 = '111' "
                                 "and __is_active = false "
                                 "and __start_date is not null "
                                 "and __end_date is not null").count()


def test_write_scd_type_2_schema_evolution(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "333",
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_schema_evolution"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_schema_evolution"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))

    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 3 == result_df.count()
    assert "address" in result_df.columns


def test_write_scd_type_2_partition(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "id_series":"100"
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "id_series":"200"
        }
    ])
    df2 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000001",
            "name": "my name",
            "id_series":"100"
        },
        
    ])
    location = f"file://{tmpdir}/test_write_scd_type_2_partition"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_partition"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        partition_columns=["id_series"],
        load_type=LoadType.TYPE_2_SCD,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))
    
    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        partition_columns=["id_series"],
        load_type=LoadType.TYPE_2_SCD,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))
    
    assert result.status == RunStatus.SUCCEEDED
    assert 2 == spark.sql(f"show partitions {schema_name}.{table_name}").count()
    assert 2 == spark.sql(f"select * from {schema_name}.{table_name} where id_series=100").count()


def test_write_scd_type_2_struct_types(tmpdir):
    # ARRANGE
    df_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('phone_number', StringType(), True),
            StructField('name', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]), True),
        ]
    )
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "address": {
                "city": "london",
                "country": "uk"
            }
        },
        {
            "id": "222",
            "phone_number": "00000000000",
            "name": "my name",
            "address": {
                "city": "london",
                "country": "uk"
            }
        }
    ], schema=df_schema)
    df2 = spark.createDataFrame([
        {
            "id": "333",
            "phone_number": "00000000000",
            "name": "my name",
            "address": {
                "city": "london",
                "country": "uk"
            }
        },
    ], schema=df_schema)
    location = f"file:{tmpdir}/test_write_scd_type_2_struct_types"
    schema_name = "test_schema"
    table_name = "test_write_scd_type_2_struct_types"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    print(result.error_details)
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 3 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


def test_write_duplicated_data_for_upsert(tmpdir):
    # ARRANGE
    df1 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "id_series": "100"
        },
    ])
    df2 = spark.createDataFrame([
        {
            "id": "111",
            "phone_number": "00000000000",
            "name": "my name",
            "id_series": "200"
        },
        {
            "id": "111",
            "phone_number": "00000000001",
            "name": "my name",
            "id_series": "300"
        },

    ])
    location = f"file://{tmpdir}/test_write_duplicated_data_for_upsert"
    schema_name = "test_schema"
    table_name = "test_write_duplicated_data_for_upsert"

    expected_message = "java.lang.UnsupportedOperationException: Cannot perform Merge as multiple source rows " \
                       "matched and attempted to modify the same target row in the Delta table in possibly " \
                       "conflicting ways. By SQL semantics of Merge, when multiple source rows match on the same " \
                       "target row, the result may be ambiguous as it is unclear which source row should be " \
                       "used to update or delete the matching target row. You can preprocess the source table to " \
                       "eliminate the possibility of multiple matches. Please refer to " \
                       "https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        schema_name=schema_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))

    assert result.status == RunStatus.FAILED
    assert result.error_message == expected_message
