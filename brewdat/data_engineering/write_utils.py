import os
import traceback
from enum import Enum, unique
from typing import List, Tuple

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from . import common_utils
from .common_utils import ReturnObject, RunStatus
from .data_quality_utils import DQ_RESULTS_COLUMN


@unique
class LoadType(str, Enum):
    """Specifies the way in which the table should be loaded.
    """
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    """Load type where the entire table is rewritten in every execution.
    Avoid whenever possible, as this is not good for large tables.
    This deletes records that are not present in the DataFrame."""
    OVERWRITE_PARTITION = "OVERWRITE_PARTITION"
    """Load type for overwriting a single partition based on partitionColumns.
    This deletes records that are not present in the DataFrame for the chosen partition.
    The df must be filtered such that it contains a single partition."""
    APPEND_ALL = "APPEND_ALL"
    """Load type where all records in the DataFrame are written into an table.
    *Attention*: use this load type only for Bronze tables, as it is bad for backfilling."""
    APPEND_NEW = "APPEND_NEW"
    """Load type where only new records in the DataFrame are written into an existing table.
    Records for which the key already exists in the table are ignored."""
    UPSERT = "UPSERT"
    """Load type where records of a df are appended as new records or update existing records based on the key.
    This does NOT delete existing records that are not included in the DataFrame."""
    TYPE_2_SCD = "TYPE_2_SCD"
    """Load type that implements the standard type-2 Slowly Changing Dimension.
    This essentially uses an upsert that keeps track of all previous versions of each record.
    For more information: https://en.wikipedia.org/wiki/Slowly_changing_dimension"""


@unique
class SchemaEvolutionMode(str, Enum):
    """Specifies the way in which schema mismatches should be handled.
    """
    FAIL_ON_SCHEMA_MISMATCH = "FAIL_ON_SCHEMA_MISMATCH"
    """Fail if the table's schema is not compatible with the DataFrame's.
    This is the default Spark behavior when no option is given."""
    ADD_NEW_COLUMNS = "ADD_NEW_COLUMNS"
    """Schema evolution through adding new columns to the target table.
    This is the same as using the option "mergeSchema"."""
    IGNORE_NEW_COLUMNS = "IGNORE_NEW_COLUMNS"
    """Drop DataFrame columns that do not exist in the table's schema.
    Does nothing if the table does not yet exist in the Hive metastore."""
    OVERWRITE_SCHEMA = "OVERWRITE_SCHEMA"
    """Overwrite the table's schema with the DataFrame's schema.
    This is the same as using the option "overwriteSchema"."""
    RESCUE_NEW_COLUMNS = "RESCUE_NEW_COLUMNS"
    """Create a new struct-type column to collect data for new columns.
    This is the same strategy used in AutoLoader's rescue mode.
    For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
    *Attention*: This schema evolution mode is not implemented on this library yet!"""


@unique
class BadRecordHandlingMode(str, Enum):
    """Specifies the way in which bad records should be handled.

    Bad records are rows where `__data_quality_issues` column exists and is not null.
    """
    WARN = "WARN"
    """Write both bad and good records to target table and warn if bad records are present."""
    REJECT = "REJECT"
    """Filter out bad records and append them to a separate error table."""


def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    database_name: str,
    table_name: str,
    load_type: LoadType,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    bad_record_handling_mode: BadRecordHandlingMode = BadRecordHandlingMode.WARN,
    time_travel_retention_days: int = 30,
    auto_broadcast_join_threshold: int = 52428800,
    enable_caching: bool = True,
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
    database_name : str
        Name of the database/schema for the table in the metastore.
        Database is created if it does not exist.
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
    bad_record_handling_mode : BrewDatLibrary.BadRecordHandlingMode, default=WARN
        Specifies the way in which bad records should be handled.
        See documentation for BrewDatLibrary.BadRecordHandlingMode.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the Delta table.
        Used to limit how many old snapshots are preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch
    auto_broadcast_join_threshold : int, default=52428800
        Configures the maximum size in bytes for a table that will be broadcast to all worker
        nodes when performing a join. Default value in bytes represents 50 MB.
    enable_caching : bool, default=True
        Cache the DataFrame so that transformations are not recomputed multiple times
        during couting, bad record handling, or writing with TYPE_2_SCD.

    Returns
    -------
    ReturnObject
        Object containing the results of a write operation.
    """
    num_records_read = 0
    num_records_loaded = 0
    old_version_number = None
    cached_df = None

    try:
        # Get original version number
        old_version_number = _get_current_delta_version_number(spark=spark, location=location)

        # Check if the table already exists with a different location
        location_mismatch = _table_exists_in_different_location(
            spark=spark,
            database_name=database_name,
            table_name=table_name,
            expected_location=location,
        )
        if location_mismatch:
            raise ValueError(
                "Metastore table already exists with a different location."
                f" To drop the existing table, use: DROP TABLE `{database_name}`.`{table_name}`"
            )

        # Use optimized writes to reduce number of small files
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
        spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", True)

        # Set maximum size in bytes for a table to be broadcast to all worker nodes when performing a join
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", auto_broadcast_join_threshold)

        # Cache the DataFrame and count source records
        if enable_caching:
            cached_df = df.cache()
        num_records_read = df.count()

        # Handle bad records
        df, num_records_errored_out = _handle_bad_records(
            spark=spark,
            df=df,
            bad_record_handling_mode=bad_record_handling_mode,
            location=location,
            database_name=database_name,
            table_name=table_name,
            time_travel_retention_days=time_travel_retention_days,
        )

        # Write data with the selected load type
        if load_type == LoadType.OVERWRITE_TABLE:
            if num_records_read == 0:
                raise ValueError("Attempted to overwrite a table with an empty dataset. Operation aborted.")

            num_records_loaded = _write_table_using_overwrite_table(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.OVERWRITE_PARTITION:
            if num_records_read == 0:
                raise ValueError("Attempted to overwrite a partition with an empty dataset. Operation aborted.")

            num_records_loaded = _write_table_using_overwrite_partition(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.APPEND_ALL:
            num_records_loaded = _write_table_using_append_all(
                spark=spark,
                df=df,
                location=location,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.APPEND_NEW:
            num_records_loaded = _write_table_using_append_new(
                spark=spark,
                df=df,
                location=location,
                key_columns=key_columns,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.UPSERT:
            num_records_loaded = _write_table_using_upsert(
                spark=spark,
                df=df,
                location=location,
                key_columns=key_columns,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        elif load_type == LoadType.TYPE_2_SCD:
            num_records_loaded = _write_table_using_type_2_scd(
                spark=spark,
                df=df,
                location=location,
                key_columns=key_columns,
                partition_columns=partition_columns,
                schema_evolution_mode=schema_evolution_mode,
            )
        else:
            raise NotImplementedError

        # Create the Hive database and table
        _create_external_hive_table(
            spark=spark,
            database_name=database_name,
            table_name=table_name,
            location=location,
        )

        # Vacuum the delta table
        _vacuum_delta_table(
            spark=spark,
            database_name=database_name,
            table_name=table_name,
            time_travel_retention_days=time_travel_retention_days,
        )

        # Get new version number
        new_version_number = _get_current_delta_version_number(spark=spark, location=location)

        if cached_df:
            cached_df.unpersist()

        return ReturnObject(
            status=RunStatus.SUCCEEDED,
            target_object=f"{database_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            num_records_errored_out=num_records_errored_out,
            old_version_number=old_version_number,
            new_version_number=new_version_number,
        )

    except Py4JJavaError as e:
        if cached_df:
            cached_df.unpersist()

        return ReturnObject(
            status=RunStatus.FAILED,
            target_object=f"{database_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            num_records_errored_out=num_records_read,
            error_message=str(e.java_exception).replace("\n", " "),
            error_details=traceback.format_exc(),
            old_version_number=old_version_number,
        )

    except Exception as e:
        if cached_df:
            cached_df.unpersist()

        return ReturnObject(
            status=RunStatus.FAILED,
            target_object=f"{database_name}.{table_name}",
            num_records_read=num_records_read,
            num_records_loaded=num_records_loaded,
            num_records_errored_out=num_records_read,
            error_message=str(e),
            error_details=traceback.format_exc(),
            old_version_number=old_version_number,
        )


def _get_current_delta_version_details(
    spark: SparkSession,
    location: str,
) -> dict:
    """Get information about the current version of a delta table given its location.

    Resulting dictionary follows history schema for delta tables.
    For more information: https://docs.databricks.com/delta/delta-utility.html#history-schema

    Returns None if given location is not a delta table.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.

    Returns
    -------
    dict
        Dictionary with information about current delta table version.
    """
    if not DeltaTable.isDeltaTable(spark, location):
        return None

    delta_table = DeltaTable.forPath(spark, location)
    history_df = delta_table.history(1)
    return history_df.first().asDict(recursive=True)


def _get_current_delta_version_number(
    spark: SparkSession,
    location: str,
) -> int:
    """Get the current version number of a delta table given its location.

    Returns None if given location is not a delta table.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.

    Returns
    -------
    int
        Current delta table version number.
    """
    current_version_details = _get_current_delta_version_details(spark=spark, location=location)
    return current_version_details.get("version") if current_version_details else None


def _handle_bad_records(
    spark: SparkSession,
    df: DataFrame,
    bad_record_handling_mode: BadRecordHandlingMode,
    location: str,
    database_name: str,
    table_name: str,
    time_travel_retention_days: int = 30,
) -> Tuple[DataFrame, int]:
    """Handle bad records on input Dataframe according to a provided mode.

    Parameters
    ----------
    spark : SparkSession
         A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    bad_record_handling_mode : BrewDatLibrary.BadRecordHandlingMode
        Specifies the way in which bad records should be handled.
        See documentation for BrewDatLibrary.BadRecordHandlingMode.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    database_name : str
        Name of the database/schema for the table in the metastore.
        Database is created if it does not exist.
    table_name : str
        Name of the table in the metastore.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the error delta table.
        Used to limit how many old snapshots are preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch

    Returns
    -------
    Tuple[DataFrame, int]
        DataFrame
            DataFrame with bad records handled according to given mode.
        int
            Number of bad records found in input Dataframe.
    """
    if DQ_RESULTS_COLUMN not in df.columns:
        return df, 0

    output_df = df
    bad_record_filter = F.col(DQ_RESULTS_COLUMN).isNotNull()
    bad_record_df = df.filter(bad_record_filter)

    if bad_record_handling_mode == BadRecordHandlingMode.REJECT:
        bad_record_count = _write_to_error_table(
            spark=spark,
            df=bad_record_df,
            location=location,
            database_name=database_name,
            table_name=table_name,
            time_travel_retention_days=time_travel_retention_days,
        )
        output_df = (
            df
            .filter(~bad_record_filter)
            .drop(DQ_RESULTS_COLUMN)
        )
    elif bad_record_handling_mode == BadRecordHandlingMode.WARN:
        bad_record_count = bad_record_df.count()
    else:
        raise NotImplementedError

    return output_df, bad_record_count


def _write_to_error_table(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    database_name: str,
    table_name: str,
    time_travel_retention_days: int,
) -> int:
    """Write bad record DataFrame to standard error table.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to write.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
        Used to determine proper error table location.
    database_name : str
        Name of the database/schema for the table in the metastore. Used to
        determine proper error table name. Database is created if it does not exist.
    table_name : str
        Name of the table in the metastore.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the error delta table.
        Used to limit how many old snapshots are preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch

    Returns
    -------
    int
        Number of records written to error location.
    """
    error_database_name = database_name + "_err"
    if database_name in location:
        error_location = location.replace(database_name, error_database_name, 1)
    else:
        error_location = location.rstrip("/") + "_err/"

    df = (
        df
        .withColumn("__data_quality_check_dt", F.current_date())
        .withColumn("__data_quality_check_ts", F.current_timestamp())
    )

    loaded_count = _write_table_using_append_all(
        spark=spark,
        df=df,
        location=error_location,
        partition_columns=["__data_quality_check_dt"],
        schema_evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )

    _create_external_hive_table(
        spark=spark,
        database_name=error_database_name,
        table_name=table_name,
        location=error_location,
    )

    _vacuum_delta_table(
        spark=spark,
        database_name=error_database_name,
        table_name=table_name,
        time_travel_retention_days=time_travel_retention_days,
    )

    return loaded_count


def _create_df_writer_with_options(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    schema_evolution_mode: SchemaEvolutionMode,
    partition_columns: List[str] = []
) -> DataFrameWriter:
    """Create DataFrameWriter for a DataFrame according to schema evolution mode.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to write.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.

    Returns
    -------
    DataFrameWriter
        DataFrameWriter using selected schema evolution mode and partition columns.
    """
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        df_writer = df.write
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        df_writer = df.write.option("mergeSchema", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        df = _drop_new_columns(spark=spark, df=df, location=location)
        df_writer = df.write
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        df_writer = df.write.option("overwriteSchema", True)
    else:
        raise NotImplementedError

    df_writer = df_writer.format("delta")

    if partition_columns:
        df_writer = df_writer.partitionBy(partition_columns)

    return df_writer


def _prepare_df_for_merge_operation(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    schema_evolution_mode: SchemaEvolutionMode,
) -> DataFrame:
    """Prepare DataFrame for a merge operation according to schema evolution mode.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to write.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.

    Returns
    -------
    DataFrame
        DataFrame modified according to schema evolution mode.
    """
    if schema_evolution_mode == SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
        pass
    elif schema_evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
    elif schema_evolution_mode == SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
        df = _drop_new_columns(spark=spark, df=df, location=location)
    elif schema_evolution_mode == SchemaEvolutionMode.OVERWRITE_SCHEMA:
        raise ValueError("OVERWRITE_SCHEMA is not supported for this load type")
    else:
        raise NotImplementedError

    return df


def _drop_new_columns(
    spark: SparkSession,
    df: DataFrame,
    location: str,
) -> DataFrame:
    """Drop DataFrame column which do not exist in the target Delta table.

    Used by IGNORE_NEW_COLUMNS schema evolution mode.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    df : DataFrame
        PySpark DataFrame to modify.
    location : str
        Absolute Delta Lake path for the physical location of the target delta table.

    Returns
    -------
    DataFrame
        DataFrame with only columns that exist in the target delta table.
        DataFrame is not modified if the delta table does not exist yet.
    """
    if not DeltaTable.isDeltaTable(spark, location):
        return df

    table_df = DeltaTable.forPath(spark, location).toDF()
    table_columns = [col.lower() for col in table_df.columns]
    new_df_columns = [col for col in df.columns if col.lower() not in table_columns]
    df = df.drop(*new_df_columns)
    return df


def _table_exists_in_different_location(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    expected_location: str
) -> bool:
    """Return whether the metastore table already exists at a different location.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    database_name : str
        Name of the database/schema for the table in the metastore.
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
    table_exists = spark.catalog._jcatalog.tableExists(f"{database_name}.{table_name}")
    if not table_exists:
        return False

    # Compare current location and expected location
    current_location = (
        spark.sql(f"DESCRIBE DETAIL `{database_name}`.`{table_name}`;")
        .select("location")
        .collect()[0][0]
    )
    return os.path.normpath(current_location) != os.path.normpath(expected_location)


def _get_latest_output_row_count(
    spark: SparkSession,
    location: str,
) -> int:
    """Obtain the latest number of output rows from the current delta version.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.

    Returns
    -------
    int
        Number of records loaded into a delta table in the latest operation.
    """
    try:
        current_version_details = _get_current_delta_version_details(spark=spark, location=location)
        operation_metrics = current_version_details["operationMetrics"]
        num_output_rows = int(operation_metrics["numOutputRows"])
        if current_version_details["operation"] == "MERGE":
            num_output_rows -= int(operation_metrics["numTargetRowsCopied"])
        return num_output_rows
    except Exception:
        return None


def _write_table_using_overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    df_writer = _create_df_writer_with_options(
        spark=spark,
        df=df,
        location=location,
        schema_evolution_mode=schema_evolution_mode,
        partition_columns=partition_columns,
    )
    df_writer.mode("overwrite").save(location)

    return _get_latest_output_row_count(spark=spark, location=location)


def _write_table_using_overwrite_partition(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    if not partition_columns:
        raise ValueError("No partition column was given")

    df_partitions = df.select(partition_columns).distinct()

    if df_partitions.count() != 1:
        raise ValueError("Found more than one partition value in the given DataFrame")

    # Build replaceWhere clause
    replace_where_clauses = []
    for partition_column, value in df_partitions.first().asDict().items():
        replace_where_clauses.append(f"`{partition_column}` = '{value}'")
    replace_where_clause = " AND ".join(replace_where_clauses)

    df_writer = _create_df_writer_with_options(
        spark=spark,
        df=df,
        location=location,
        schema_evolution_mode=schema_evolution_mode,
        partition_columns=partition_columns,
    )
    df_writer.mode("overwrite").option("replaceWhere", replace_where_clause).save(location)

    return _get_latest_output_row_count(spark=spark, location=location)


def _write_table_using_append_all(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    df_writer = _create_df_writer_with_options(
        spark=spark,
        df=df,
        location=location,
        partition_columns=partition_columns,
        schema_evolution_mode=schema_evolution_mode,
    )
    df_writer.mode("append").save(location)

    return _get_latest_output_row_count(spark=spark, location=location)


def _write_table_using_append_new(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    if not key_columns:
        raise ValueError("No key column was given")

    if not DeltaTable.isDeltaTable(spark, location):
        print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
        return _write_table_using_append_all(
            spark=spark,
            df=df,
            location=location,
            partition_columns=partition_columns,
            schema_evolution_mode=schema_evolution_mode,
        )

    df = _prepare_df_for_merge_operation(
        spark=spark,
        df=df,
        location=location,
        schema_evolution_mode=schema_evolution_mode,
    )

    # Build merge condition using null-safe equal to avoid duplicating null keys
    merge_condition_parts = [f"source.`{col}` <=> target.`{col}`" for col in key_columns]
    merge_condition = " AND ".join(merge_condition_parts)

    # Write to the delta table
    delta_table = DeltaTable.forPath(spark, location)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )

    return _get_latest_output_row_count(spark=spark, location=location)


def _write_table_using_upsert(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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
    partition_columns : List[str], default=[]
        The names of the columns used to partition the table.
    schema_evolution_mode : BrewDatLibrary.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
        Specifies the way in which schema mismatches should be handled.
        See documentation for BrewDatLibrary.SchemaEvolutionMode.

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    if not key_columns:
        raise ValueError("No key column was given")

    if not DeltaTable.isDeltaTable(spark, location):
        print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
        return _write_table_using_append_all(
            spark=spark,
            df=df,
            location=location,
            partition_columns=partition_columns,
            schema_evolution_mode=schema_evolution_mode,
        )

    df = _prepare_df_for_merge_operation(
        spark=spark,
        df=df,
        location=location,
        schema_evolution_mode=schema_evolution_mode,
    )

    # Build merge condition using null-safe equal to avoid duplicating null keys
    merge_condition_parts = [f"source.`{col}` <=> target.`{col}`" for col in key_columns]
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

    return _get_latest_output_row_count(spark=spark, location=location)


def _write_table_using_type_2_scd(
    spark: SparkSession,
    df: DataFrame,
    location: str,
    key_columns: List[str] = [],
    partition_columns: List[str] = [],
    schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
) -> int:
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

    Returns
    -------
    int
        Number of loaded records into target delta table location.
    """
    if not key_columns:
        raise ValueError("No key column was given")

    df = _generate_type_2_scd_metadata_columns(df)

    if not DeltaTable.isDeltaTable(spark, location):
        print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
        return _write_table_using_append_all(
            spark=spark,
            df=df,
            location=location,
            partition_columns=partition_columns,
            schema_evolution_mode=schema_evolution_mode,
        )

    # Prepare DataFrame for merge operation
    table_df = spark.read.format("delta").load(location)
    merge_df = _merge_dataframe_for_type_2_scd(source_df=df, target_df=table_df, key_columns=key_columns)
    merge_df = _prepare_df_for_merge_operation(
        spark=spark,
        df=merge_df,
        location=location,
        schema_evolution_mode=schema_evolution_mode,
    )

    # Build merge condition using null-safe equal to avoid duplicating null keys
    merge_condition = "source.__hash_key <=> target.__hash_key"
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

    return _get_latest_output_row_count(spark=spark, location=location)


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

    Notes
    -----
    __hash_key uses the Base64 representation of the MD5 hash of all columns,
    except for metadata columns (those whose name starts with two underscores).
    """
    all_cols = common_utils.list_non_metadata_columns(df)
    df = df.withColumn(
        "__hash_key",
        F.substring(F.base64(F.unhex(F.md5(F.to_json(F.struct(*all_cols))))), 0, 22)
    )
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


def _create_external_hive_table(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    location: str,
):
    """Create external delta table on Hive metastore if it does not exist yet.
    Also create the database if it does not exist yet.

    Parameters
    ----------
    spark: SparkSession
        A Spark session.
    database_name : str
        Name of the database/schema for the external table in the metastore.
        Database is created if it does not exist.
    table_name : str
        Name of the external table in the metastore.
    location : str
        Absolute Delta Lake path for the physical location of this delta table.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`;")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}`
        USING DELTA
        LOCATION '{location}';
    """)


def _vacuum_delta_table(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    time_travel_retention_days: int = 30,
):
    """Vacuum a Delta table using given retention period.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    database_name : str
        Name of the database/schema for the table in the metastore.
    table_name : str
        Name of the table in the metastore.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the Delta table.
        Used to limit the number of old snapshots preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch
    """
    spark.sql(f"""
        ALTER TABLE `{database_name}`.`{table_name}`
        SET TBLPROPERTIES (
            'delta.deletedFileRetentionDuration' = 'interval {time_travel_retention_days} days',
            'delta.logRetentionDuration' = 'interval {time_travel_retention_days} days'
        );
    """)
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", True)
    spark.sql(f"VACUUM `{database_name}`.`{table_name}`;")
