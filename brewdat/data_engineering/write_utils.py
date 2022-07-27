import os
import traceback
from enum import Enum, unique
from typing import List

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .common_utils import ReturnObject, RunStatus


@unique
class LoadType(str, Enum):
    """Specifies the way in which the table should be loaded.

    OVERWRITE_TABLE: Load type where the entire table is rewritten in every execution.
    Avoid whenever possible, as this is not good for large tables.
    This deletes records that are not present in the DataFrame.

    OVERWRITE_PARTITION: Load type for overwriting a single partition based on partitionColumns.
    This deletes records that are not present in the DataFrame for the chosen partition.
    The df must be filtered such that it contains a single partition.

    APPEND_ALL: Load type where all records in the DataFrame are written into an table.
    *Attention*: use this load type only for Bronze tables, as it is bad for backfilling.

    APPEND_NEW: Load type where only new records in the DataFrame are written into an existing table.
    Records for which the key already exists in the table are ignored.

    UPSERT: Load type where records of a df are appended as new records or update existing records based on the key.
    This does NOT delete existing records that are not included in the DataFrame.

    TYPE_2_SCD: Load type that implements the standard type-2 Slowly Changing Dimension implementation.
    This essentially uses an upsert that keeps track of all previous versions of each record.
    For more information: https://en.wikipedia.org/wiki/Slowly_changing_dimension
    """
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    OVERWRITE_PARTITION = "OVERWRITE_PARTITION"
    APPEND_ALL = "APPEND_ALL"
    APPEND_NEW = "APPEND_NEW"
    UPSERT = "UPSERT"
    TYPE_2_SCD = "TYPE_2_SCD"


@unique
class SchemaEvolutionMode(str, Enum):
    """Specifies the way in which schema mismatches should be handled.

    FAIL_ON_SCHEMA_MISMATCH: Fail if the table's schema is not compatible with the DataFrame's.
    This is the default Spark behavior when no option is given.

    ADD_NEW_COLUMNS: Schema evolution through adding new columns to the target table.
    This is the same as using the option "mergeSchema".

    IGNORE_NEW_COLUMNS: Drop DataFrame columns that do not exist in the table's schema.
    Does nothing if the table does not yet exist in the Hive metastore.

    OVERWRITE_SCHEMA: Overwrite the table's schema with the DataFrame's schema.
    This is the same as using the option "overwriteSchema".

    RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
    This is the same strategy used in AutoLoader's rescue mode.
    For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
    *Attention*: This schema evolution mode is not implemented on this library yet!
    """
    FAIL_ON_SCHEMA_MISMATCH = "FAIL_ON_SCHEMA_MISMATCH"
    ADD_NEW_COLUMNS = "ADD_NEW_COLUMNS"
    IGNORE_NEW_COLUMNS = "IGNORE_NEW_COLUMNS"
    OVERWRITE_SCHEMA = "OVERWRITE_SCHEMA"
    RESCUE_NEW_COLUMNS = "RESCUE_NEW_COLUMNS"


def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    schema_name: str,
    table_name: str,
    load_type: LoadType,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    time_travel_retention_days: int = 30,
) -> ReturnObject:
    """Write the DataFrame as a delta table.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    schema_name : str
        Name of the schema/database for the table in the metastore.
        Schema is created if it does not exist.
    table_name : str
        Name of the table in the metastore.
    load_type : BrewDatLibrary.LoadType
        Specifies the way in which the table should be loaded.
        See documentation for BrewDatLibrary.LoadType.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record in the table.
        Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the Delta table.
        Used to limit how many old snapshots are preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch

    Returns
    -------
    ReturnObject
        Object containing the results of a write operation.
    """
    # TODO: refactor
    num_records_read = 0
    num_records_loaded = 0

    try:
        # Check if the table already exists with a different location
        location_mismatch = _table_exists_in_different_location(
            spark=spark,
            schema_name=schema_name,
            table_name=table_name,
            expected_location=location,
        )
        if location_mismatch:
            raise ValueError(
                "Metastore table already exists with a different location."
                f" To drop the existing table, use: DROP TABLE `{schema_name}`.`{table_name}`"
            )

        # Table must exist if we are merging data
        if load_type not in (LoadType.APPEND_ALL, LoadType.TYPE_2_SCD) and not DeltaTable.isDeltaTable(spark, location):
            print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
            load_type = LoadType.APPEND_ALL

        # Use optimized writes to create less small files
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
        spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", True)

        # Count source records
        num_records_read = df.count()

        # Write data with the selected load_type
        if load_type == LoadType.OVERWRITE_TABLE:
            if num_records_read == 0:
                raise ValueError("Attempted to overwrite a table with an empty dataset. Operation aborted.")

            _write_table_using_overwrite_table(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.OVERWRITE_PARTITION:
            if not partition_columns:
                raise ValueError("No partition column was given")

            if num_records_read == 0:
                raise ValueError("Attempted to overwrite a partition with an empty dataset. Operation aborted.")

            _write_table_using_overwrite_partition(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.APPEND_ALL:
            _write_table_using_append_all(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.APPEND_NEW:
            if not key_columns:
                raise ValueError("No key column was given")

            _write_table_using_append_new(
                spark=spark,
                df=df,
                location=location,
                key_columns=key_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.UPSERT:
            if not key_columns:
                raise ValueError("No key column was given")

            _write_table_using_upsert(
                spark=spark,
                df=df,
                location=location,
                key_columns=key_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.TYPE_2_SCD:
            if not key_columns:
                raise ValueError("No key column was given")

            _write_table_using_type_2_scd(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                key_columns=key_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        else:
            raise NotImplementedError

        # Find out how many records we have just written
        delta_table = DeltaTable.forPath(spark, location)
        last_history_df = (
            delta_table
            .history(1)
        )
        history_df = last_history_df.select(F.col("operationMetrics.numOutputRows").cast("int"))
        num_records_loaded = history_df.first()[0]

        # Create the Hive database and table
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`;")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}`
            USING DELTA
            LOCATION '{location}';
        """)

        # Vacuum the delta table
        spark.sql(f"""
            ALTER TABLE `{schema_name}`.`{table_name}`
            SET TBLPROPERTIES (
                'delta.deletedFileRetentionDuration' = 'interval {time_travel_retention_days} days',
                'delta.logRetentionDuration' = 'interval {time_travel_retention_days} days'
            );
        """)
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", True)
        spark.sql(f"VACUUM `{schema_name}`.`{table_name}`;")

        return ReturnObject(
            status=RunStatus.SUCCEEDED,
            target_object=f"{schema_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            last_history_df = last_history_df
        )

    except Exception as e:
        return ReturnObject(
            status=RunStatus.FAILED,
            target_object=f"{schema_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            error_message=str(e),
            error_details=traceback.format_exc(),
            last_history_df = last_history_df,
        )


def _table_exists_in_different_location(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
    expected_location: str
) -> bool:
    """Return whether the metastore table already exists at a different location.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    schema_name : str
        Name of the schema/database for the table in the metastore.
    table_name : str
        Name of the table in the metastore.
    expected_location : str
        Absolute Delta Lake path for the expected physical location of this delta table.

    Returns
    -------
    bool
        True if table exists and its location is NOT the expected location.
    """
    # Check if the table exists
    table_exists = spark.catalog._jcatalog.tableExists(f"{schema_name}.{table_name}")
    if not table_exists:
        return False

    # Compare current location and expected location
    current_location = (
        spark.sql(f"DESCRIBE DETAIL `{schema_name}`.`{table_name}`;")
        .select("location")
        .collect()[0][0]
    )
    return os.path.normpath(current_location) != os.path.normpath(expected_location)


def _write_table_using_overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using OVERWRITE_TABLE.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    df_writer = (
        df.write
        .format("delta")
        .mode("overwrite")
    )

    # Set partition options
    if partition_columns:
        df_writer = df_writer.partitionBy(partition_columns)

    # Set schema_evolution_mode options
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        df_writer = df_writer.option("mergeSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in df.columns if col not in table_columns]
            df = df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        df_writer = df_writer.option("overwriteSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError

    # Write to the delta table
    df_writer.save(location)


def _write_table_using_overwrite_partition(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using OVERWRITE_PARTITION.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    df_partitions = df.select(partition_columns).distinct()

    if df_partitions.count() != 1:
        raise ValueError("Found more than one partition value in the given DataFrame")

    # Build replaceWhere clause
    replace_where_clauses = []
    for partition_column, value in df_partitions.first().asDict().items():
        replace_where_clauses.append(f"`{partition_column}` = '{value}'")
    replace_where_clause = " AND ".join(replace_where_clauses)

    df_writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where_clause)
    )

    # Set partition options
    if partition_columns:
        df_writer = df_writer.partitionBy(partition_columns)

    # Set schema_evolution_mode options
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        df_writer = df_writer.option("mergeSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in df.columns if col not in table_columns]
            df = df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        df_writer = df_writer.option("overwriteSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError

    # Write to the delta table
    df_writer.save(location)


def _write_table_using_append_all(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using APPEND_ALL.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    df_writer = (
        df.write
        .format("delta")
        .mode("append")
    )

    # Set partition options
    if partition_columns:
        df_writer = df_writer.partitionBy(partition_columns)

    # Set schema_evolution_mode options
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        df_writer = df_writer.option("mergeSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in df.columns if col not in table_columns]
            df = df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        df_writer = df_writer.option("overwriteSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError

    # Write to the delta table
    df_writer.save(location)


def _write_table_using_append_new(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using APPEND_NEW.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record in the table.
        Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    # Set schema_evolution_mode options
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in df.columns if col not in table_columns]
            df = df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        raise ValueError("OVERWRITE_SCHEMA is not supported in APPEND_NEW load type")
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError

    # Build merge condition
    merge_condition_parts = [f"source.`{col}` = target.`{col}`" for col in key_columns]
    merge_condition = " AND ".join(merge_condition_parts)

    # Write to the delta table
    delta_table = DeltaTable.forPath(spark, location)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_table_using_upsert(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using UPSERT.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record in the table.
        Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    # Set schema_evolution_mode options
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in df.columns if col not in table_columns]
            df = df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        raise ValueError("OVERWRITE_SCHEMA is not supported in UPSERT load type")
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError

    # Build merge condition
    merge_condition_parts = [f"source.`{col}` = target.`{col}`" for col in key_columns]
    merge_condition = " AND ".join(merge_condition_parts)

    # Write to the delta table
    delta_table = DeltaTable.forPath(spark, location)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_table_using_type_2_scd(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using TYPE_2_SCD.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to write.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record in the table.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    df = _generate_type_2_scd_metadata_columns(df)

    if DeltaTable.isDeltaTable(spark, location):
        # Prepare DataFrame for merge operation
        table_df = spark.read.format("delta").load(location)
        merge_df = _merge_dataframe_for_type_2_scd(source_df=df, target_df=table_df, key_columns=key_columns)

        # Set schema_evolution_mode options
        if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
        elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            table_columns = DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns = [col for col in merge_df.columns if col not in table_columns]
            merge_df = merge_df.drop(*new_df_columns)
        elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
            raise ValueError("OVERWRITE_SCHEMA is not supported in TYPE_2_SCD load type")
        elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Build merge conditions
        merge_condition = "source.__hash_key = target.__hash_key"
        update_condition = "source.__is_active = false and target.__is_active = true"

        # Write to the delta table
        delta_table = DeltaTable.forPath(spark, location)
        (
            delta_table.alias("target")
            .merge(merge_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition=update_condition,
                set={
                    "__end_date": F.current_timestamp(),
                    "__is_active": F.lit(False),
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        _write_table_using_append_all(
            spark=spark,
            df=df,
            location=location,
            partition_columns=partition_columns,
            schema_evolution_mode=schema_evolution_mode
        )


def _generate_type_2_scd_metadata_columns(
    df: DataFrame,
) -> DataFrame:
    """Create or replace Type-2 SCD metadata columns in the given DataFrame.

    The following columns are created/replaced:
        - __hash_key: used as both a surrogate key and a checksum.
        - __start_date: timestamp of when the record started being effective.
        - __end_date: timestamp of when the record ceased being effective.
        - __is_active: whether the record is currently effective.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to modify.

    Returns
    -------
    DataFrame
        Pyspark Dataframe with new Type-2 SCD metadata columns.
    """
    all_cols = [col for col in df.columns if not col.startswith("__")]
    df = df.withColumn("__hash_key", F.md5(F.to_json(F.struct(*all_cols))))
    df = df.withColumn("__start_date", F.current_timestamp())
    df = df.withColumn("__end_date", F.lit(None).cast("timestamp"))
    df = df.withColumn("__is_active", F.lit(True))
    return df


def _merge_dataframe_for_type_2_scd(
    source_df: DataFrame,
    target_df: DataFrame,
    key_columns: List[str] = [],
) -> DataFrame:
    """Create the merge DataFrame for TYPE_2_SCD load type.

    This DataFrame contains all the hash keys of target records which
    should be deactivated. Their __is_active column is set to False.

    Additionally, it also contains all the new records that will be inserted
    if, and only if, their hash key is not already active in the target table.
    Their __is_active columns is set to True.

    Parameters
    ----------
    source_df : DataFrame
        PySpark DataFrame with new records for updating the target table.
    target_df : DataFrame
        PySpark DataFrame containing target table records.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record in the table.

    Returns
    -------
    Dataframe
        Pyspark Dataframe ready to be merged into target table.

    Examples
    --------
    source_df:
    +----+-------+-------------+------------+--------------+------------+-------------+
    | id | name  | status_code | __hash_key | __start_date | __end_date | __is_active |
    +----+-------+-------------+------------+--------------+------------+-------------+
    | 1  | Alice | 42          | 11aa22bb   | 2022-01-02   | null       | True        |
    | 2  | Bob   | 1           | a1b2c3d4   | 2022-01-02   | null       | True        |
    +----+-------+-------------+------------+--------------+------------+-------------+

    target_df:
    +----+-------+-------------+------------+--------------+------------+-------------+
    | id | name  | status_code | __hash_key | __start_date | __end_date | __is_active |
    +----+-------+-------------+------------+--------------+------------+-------------+
    | 1  | Alice | 1           | 0a0b0c0d   | 2022-01-01   | null       | True        |
    +----+-------+-------------+------------+--------------+------------+-------------+

    key_columns:
    [id]

    result:
    +------------+-------------+------+-------+-------------+--------------+------------+
    | __hash_key | __is_active |  id  | name  | status_code | __start_date | __end_date |
    +------------+-------------+------+-------+-------------+--------------+------------+
    | 0a0b0c0d   | False       | null | null  | null        | null         | null       |
    | 11aa22bb   | True        |  1   | Alice | 42          | 2022-01-02   | null       |
    | a1b2c3d4   | True        |  2   | Bob   | 1           | 2022-01-02   | null       |
    +------------+-------------+------+-------+-------------+--------------+------------+
    """
    # Build join condition
    join_condition = [F.col(f"source.`{col}`") == F.col(f"target.`{col}`") for col in key_columns]
    join_condition += [F.col("source.__hash_key") != F.col("target.__hash_key")]

    # update_df contains only the hash keys for target records which must be deactivated
    # All of these records have __is_active set to False
    update_df = (
        target_df.alias("target")
        .filter(F.col("__is_active"))
        .join(
            source_df.alias("source"),
            on=join_condition,
            how="left_semi",
        )
        .select("__hash_key")
        .withColumn("__is_active", F.lit(False))
    )

    # merge_df is the DataFrame we will use in the merge operation
    # It contains the hash keys for target records which should be deactivated (__is_active = False)
    # As well as the new records that should be inserted (__is_active = True)
    merge_df = update_df.unionByName(source_df, allowMissingColumns=True)

    return merge_df
