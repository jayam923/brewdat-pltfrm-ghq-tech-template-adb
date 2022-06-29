import traceback
from enum import Enum, unique
from typing import List
from datetime import datetime

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
    *Attention*: This load type is not implemented on this library yet!
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
        The names of the columns used to uniquely identify each record the table.
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
        # Count source records
        num_records_read = df.count()

        # Table must exist if we are merging data
        if load_type not in (LoadType.APPEND_ALL,LoadType.TYPE_2_SCD) and not DeltaTable.isDeltaTable(spark, location):
            print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
            load_type = LoadType.APPEND_ALL

        # Use optimized writes to create less small files
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
        spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", True)

        # Set load_type options
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
                
            _write_table_using_scd2(
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
        history_df = (
            delta_table
            .history(1)
            .select(F.col("operationMetrics.numOutputRows").cast("int"))
        )
        num_records_loaded = history_df.first()[0]

        # Create the Hive database and table
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`;")

        # TODO: drop existing table if its location has changed
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
        )

    except Exception as e:
        return ReturnObject(
            status=RunStatus.FAILED,
            target_object=f"{schema_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            error_message=str(e),
            error_details=traceback.format_exc(),
        )


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
        The names of the columns used to uniquely identify each record the table.
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
        The names of the columns used to uniquely identify each record the table.
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

def _write_table_using_scd2(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str]=[],
    partition_columns: List[str]=[],
    schema_evolution_mode: SchemaEvolutionMode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
):
    """Write the DataFrame using SCD Type-2.
    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record the table.
        Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    """
    # TODO: refactor
    df=__generating_columns_for_scd(df)
    if DeltaTable.isDeltaTable(spark, location):
        filtered_df=spark.read.format("delta").option("path", location).load()
        df=__filter_for_scd2(df1=df, df2=filtered_df, keys=key_columns)
    df_writer=(
        df.write
        .format("delta")
        .mode("append")
    )
    # Set partition options
    if partition_columns:
        df_writer=df_writer.partitionBy(partition_columns)
        
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        df_writer=df_writer.option("mergeSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        if DeltaTable.isDeltaTable(spark, location):
            table_columns=DeltaTable.forPath(spark, location).toDF().columns
            new_df_columns=[col for col in df.columns if col not in table_columns]
            df=df.drop(*new_df_columns)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        df_writer=df_writer.option("overwriteSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
        raise NotImplementedError
    else:
        raise NotImplementedError
        
    if DeltaTable.isDeltaTable(spark, location):
        current_ts=F.current_timestamp()
        merge_condition_parts=[f"source.`{col}`=target.`{col}`" for col in key_columns]
        merge_condition_tmp=" AND ".join(merge_condition_parts)
        merge_condition=f"{merge_condition_tmp} AND target.`__active_flag` == True "
        delta_table=DeltaTable.forPath(spark, location)
        (delta_table.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdate(set ={"__end_date": current_ts, "__active_flag":"false"}).execute())
        
    df_writer.save(location)
    
def __generating_columns_for_scd(df):
    """
    We need to generate __start_date, __end_date, __row_checsum to be used as surrogate key and __active_flag
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to modify
    """
    all_cols=[x for x in df.columns if not x.startswith("__")]
    df=df.withColumn('__hash_key', F.md5(F.concat_ws('', *all_cols)))
    current_ts=F.current_timestamp()
    df=df.withColumn("__start_date", current_ts)
    df=df.withColumn("__end_date", F.lit(None).astype("timestamp"))
    df=df.withColumn("__active_flag", F.lit(True).astype("boolean"))
    return df

def __filter_for_scd2(df1, df2, keys):
    """
    This function helps filter the data to be processed for SCD Type-2.
    Left outer join is performed and new checksum values(rows that have updates)
    and rows to be inserted are filtered.
    Rows that have same checksum are omitted
    Parameters
    ----------
    df1 : DataFrame
        PySpark DataFrame to modify
    df2 : DataFrame
        PySpark DataFrame to modify
    key_columns : List[str], default=[]
        The names of the columns used to uniquely identify each record the table.
        Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
    """
    cond=[F.col(f"t1.{x}") == F.col(f"t2.{x}")  for x in keys]
    df2=df2.filter(F.col("__active_flag") == 'true')
    return (
        df1.alias("t1").
        join(df2.alias("t2"),cond,how="left_outer").
        filter(
            (F.col("t2.__hash_key").isNull())
            |
            (F.col("t2.__hash_key") != F.col("t1.__hash_key"))).
        select("t1.*"))
    