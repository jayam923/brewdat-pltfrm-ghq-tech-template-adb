from test.spark_test import spark

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from brewdat.data_engineering.write_utils import LoadType, write_delta_table, SchemaEvolutionMode, BadRecordHandlingMode
from brewdat.data_engineering.common_utils import RunStatus


database_name = "test_schema"


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
    database_name = "test_schema"
    table_name = "test_write_delta_table_append_all"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_location_exists"
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    print(vars(result))
    
    new_location = f"file://{tmpdir}/test_location_exists_new_location"
    
    result_1 = write_delta_table(
        spark=spark,
        df=df,
        location=new_location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
    )
    print(vars(result_1))
    
    assert result_1.status == RunStatus.FAILED
    assert result_1.error_message == f"Metastore table already exists with a different location. To drop the existing table, use: DROP TABLE `{database_name}`.`{table_name}`"


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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_first_write"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_only_new_ids"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_only_updates"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_same_id_same_data"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_updates_and_new_records"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_multiple_keys"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id", "id2"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_schema_evolution"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
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
        database_name=database_name,
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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_partition"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
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
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        partition_columns=["id_series"],
        load_type=LoadType.TYPE_2_SCD,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result))
    
    assert result.status == RunStatus.SUCCEEDED
    assert 2 == spark.sql(f"show partitions {database_name}.{table_name}").count()
    assert 2 == spark.sql(f"select * from {database_name}.{table_name} where id_series=100").count()


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
    database_name = "test_schema"
    table_name = "test_write_scd_type_2_struct_types"

    # ACT
    result = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )
    print(vars(result))

    # ASSERT
    result_df = spark.table(result.target_object)
    assert result.status == RunStatus.SUCCEEDED
    assert 3 == result_df.count()
    assert 1 == result_df.filter("id = '111' "
                                 "and __is_active = true "
                                 "and __start_date is not null "
                                 "and __end_date is null").count()


expected_message_for_merging_duplicated_records = "java.lang.UnsupportedOperationException: Cannot perform Merge as " \
        "multiple source rows matched and attempted to modify the same target row in the Delta table in possibly " \
        "conflicting ways. By SQL semantics of Merge, when multiple source rows match on the same " \
        "target row, the result may be ambiguous as it is unclear which source row should be " \
        "used to update or delete the matching target row. You can preprocess the source table to " \
        "eliminate the possibility of multiple matches. Please refer to "


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
        {
            "id": "222",
            "phone_number": "00000000001",
            "name": "my name",
            "id_series": "300"
        },

    ])
    location = f"file://{tmpdir}/test_write_duplicated_data_for_upsert"
    database_name = "test_schema"
    table_name = "test_write_duplicated_data_for_upsert"

    # ACT
    result_1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result_1))

    result_2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    print(vars(result_2))

    # ASSERT
    assert result_1.status == RunStatus.SUCCEEDED
    result_df = spark.table(result_2.target_object)
    assert result_2.status == RunStatus.FAILED
    assert result_2.error_message.startswith(expected_message_for_merging_duplicated_records)
    assert 1 == result_df.count()


def test_write_delta_table_append_new(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
            },])
    
    df2 = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
        },

        {
            "phone_number": "1111111111",
            "name": "Joao",
            "address": "Street 01"
            }
    ])
    table_name = "test_write_delta_table_append_new"
    location = f"file://{tmpdir}/{database_name}/{table_name}"

    # ACT
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_NEW,
        key_columns=['phone_number'],
        )
    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_NEW,
        key_columns=['phone_number'],
        )
    print(vars(result))
    
    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 2 == result_df.count()
    

def test_write_delta_table_overwrite_table(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
            },])
    
    df2 = spark.createDataFrame([
        {
            "phone_number": "111111111111",
            "name": "my name",
            "address": "my address"
        },

    ])
    location = f"file://{tmpdir}/test_write_delta_table_overwrite_table"
    database_name = "test_schema"
    table_name = "test_write_delta_table_overwrite_table"
    # ACT
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.OVERWRITE_TABLE,
        )
    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.OVERWRITE_TABLE,
        key_columns=['phone_number'],
        )
    print(vars(result))
    
    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 1 == result_df.count()


def test_write_delta_table_overwrite_partition(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
            },])
    
    df2 = spark.createDataFrame([
        {
            "phone_number": "111111111111",
            "name": "my name",
            "address": "my address"
        },

    ])
    location = f"file://{tmpdir}/test_write_delta_table_overwrite_partition"
    database_name = "test_schema"
    table_name = "test_write_delta_table_overwrite_partition"
    # ACT
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.OVERWRITE_PARTITION,
        partition_columns=['phone_number'],
        )
    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.OVERWRITE_PARTITION,
        partition_columns=['phone_number'],
        )
    print(vars(result))
    
    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 2 == result_df.count()


def test_write_delta_table_upsert(tmpdir):
    # ARRANGE
    df = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "my address"
            },])
    
    df2 = spark.createDataFrame([
        {
            "phone_number": "00000000000",
            "name": "my name",
            "address": "Street"
        },

    ])
    location = f"file://{tmpdir}/test_write_delta_table_upsert"
    database_name = "test_schema"
    table_name = "test_write_delta_table_upsert"
    # ACT
    
    result = write_delta_table(
        spark=spark,
        df=df,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.UPSERT,
        key_columns=['phone_number'],
        )
    result = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.UPSERT,
        key_columns=['phone_number'],
        )
    print(vars(result))
    
    # ASSERT
    assert result.status == RunStatus.SUCCEEDED
    result_df = spark.table(result.target_object)
    assert 1 == result_df.count()

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
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
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
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.UPSERT,
    )

    assert 1 == result1.num_records_loaded
    assert 2 == result2.num_records_loaded
    assert 2 == result3.num_records_loaded


def test_append_new_load_count(tmpdir):
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

    df4 = spark.createDataFrame([
        {"id": "333", "phone_number": "00000000001", },
        {"id": "444", "phone_number": "00000000001", },
    ])

    location = f"file://{tmpdir}/test_append_new_load_count"
    table_name = "test_append_new_load_count"

    print("############# ROUND 1")
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print("############# ROUND 4")
    result4 = write_delta_table(
        spark=spark,
        df=df4,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.APPEND_NEW,
    )

    print(vars(result2))
    assert 1 == result1.num_records_loaded
    assert 1 == result2.num_records_loaded
    assert 0 == result3.num_records_loaded
    assert 2 == result4.num_records_loaded


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
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )

    print("############# ROUND 2")
    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        key_columns=["id"],
        load_type=LoadType.TYPE_2_SCD,
    )

    print("############# ROUND 3")
    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
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
            StructField('__data_quality_issues', ArrayType(StringType()), True),
        ]
    )

    df1 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": None},
    ])
    df2 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
    ])
    df3 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000", "__data_quality_issues": None},
    ], schema=df_schema)
    df4 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
    ])
    table_name = "test_write_bad_records_write_to_error_location_mode"
    location = f"file://{tmpdir}/{database_name}/{table_name}"

    # ACT
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.REJECT,
    )

    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.REJECT,
    )

    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.REJECT,
    )

    result4 = write_delta_table(
        spark=spark,
        df=df4,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.REJECT,
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

    result_df = spark.table(f"{database_name}.{table_name}")
    assert "__data_quality_issues" not in result_df.columns
    assert 4 == result_df.count()

    error_df = spark.table(f"{database_name}_err.{table_name}")
    assert 2 == error_df.count()


def test_write_bad_records_ignore_mode(tmpdir):
    # ARRANGE
    df_schema = StructType(
        [
            StructField('id', StringType(), True),
            StructField('phone_number', StringType(), True),
            StructField('__data_quality_issues', ArrayType(StringType()), True),
        ]
    )

    df1 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": None},
    ])
    df2 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000"},
    ])
    df3 = spark.createDataFrame([
        {"id": "000", "phone_number": "00000000000", "__data_quality_issues": None},
    ], schema=df_schema)
    df4 = spark.createDataFrame([
        {"id": "111", "phone_number": "00000000000", "__data_quality_issues": ["There is a DQ issue"]},
    ])
    location = f"file://{tmpdir}/test_write_bad_records_ignore_mode"
    table_name = "test_write_bad_records_ignore_mode"

    # ACT
    result1 = write_delta_table(
        spark=spark,
        df=df1,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.WARN,
    )

    result2 = write_delta_table(
        spark=spark,
        df=df2,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.WARN,
    )

    result3 = write_delta_table(
        spark=spark,
        df=df3,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.WARN,
    )

    result4 = write_delta_table(
        spark=spark,
        df=df4,
        location=location,
        database_name=database_name,
        table_name=table_name,
        load_type=LoadType.APPEND_ALL,
        bad_record_handling_mode=BadRecordHandlingMode.WARN,
    )

    # ASSERT
    print(vars(result1))
    assert RunStatus.SUCCEEDED == result1.status
    assert 3 == result1.num_records_read
    assert 3 == result1.num_records_loaded
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
    assert 1 == result4.num_records_loaded
    assert 1 == result4.num_records_errored_out

    result_df = spark.table(f"{database_name}.{table_name}")
    assert "__data_quality_issues" in result_df.columns
    assert 6 == result_df.count()

