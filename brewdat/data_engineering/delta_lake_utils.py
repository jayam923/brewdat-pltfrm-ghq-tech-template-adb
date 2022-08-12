from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def create_hive_table(
        spark: SparkSession,
        schema_name: str,
        table_name: str,
        location: str
):
    """Create Hive table on metastore from a Delta location.

    Parameters
    ----------
    spark: SparkSession
        A Spark session.
    schema_name : str
        Name of the schema/database for the table in the metastore.
        Schema is created if it does not exist.
    table_name : str
        Name of the table in the metastore.
    location: str
        Absolute Delta Lake path for the physical location of this delta table.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`;")
    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}`
                USING DELTA
                LOCATION '{location}';
            """)


def vacuum_table(
        spark: SparkSession,
        schema_name: str,
        table_name: str,
        time_travel_retention_days: int,
):
    """Vacuum a Delta table.

    Parameters
    ----------
    spark: SparkSession
        A Spark session.
    schema_name : str
        Name of the schema/database for the table in the metastore.
    table_name : str
        Name of the table in the metastore.
    time_travel_retention_days : int, default=30
        Number of days for retaining time travel data in the Delta table.
        Used to limit how many old snapshots are preserved during the VACUUM operation.
        For more information: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch
    """
    spark.sql(f"""
                ALTER TABLE `{schema_name}`.`{table_name}`
                SET TBLPROPERTIES (
                    'delta.deletedFileRetentionDuration' = 'interval {time_travel_retention_days} days',
                    'delta.logRetentionDuration' = 'interval {time_travel_retention_days} days'
                );
            """)
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", True)
    spark.sql(f"VACUUM `{schema_name}`.`{table_name}`;")


def get_latest_delta_version_details(
        spark: SparkSession,
        location: str
) -> dict:
    """Gets information about latest version of provided Delta table location.

    The structure of result dictionary follows history schema for delta tables,
    For more information:https://docs.databricks.com/delta/delta-utility.html#history-schema

    In case provided location is not a Delta Table, None is returned.

    Parameters
    ----------
    spark: SparkSession
        A Spark session.
    location: str
        Absolute Delta Lake path for the physical location of this delta table.

    Returns
    -------
    dict
        Dictionary with information about latest Delta table version.

    """
    if DeltaTable.isDeltaTable(spark, location):
        delta_table = DeltaTable.forPath(spark, location)
        history_df = delta_table.history(1)
        return history_df.collect()[0].asDict(recursive=True)

    else:
        return None


def get_latest_delta_version_number(
    spark: SparkSession,
    location: str
) -> int:
    """Get latest version of Delta table in location.

    Parameters
    ----------
    spark: SparkSession
        A Spark session.
    location: str
        Absolute Delta Lake path for the physical location of this delta table.

    Returns
    -------
    int
        Latest Delta table version.
    """
    latest_delta_version = get_latest_delta_version_details(spark=spark, location=location)
    return latest_delta_version["version"] if latest_delta_version else None

