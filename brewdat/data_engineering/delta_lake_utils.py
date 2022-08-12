from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def create_hive_table(
        spark: SparkSession,
        schema_name: str,
        table_name: str,
        location: str
):
    # TODO: pydoc
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
    # TODO: pydoc
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
    """Gets information about latest delta version of provided delta table location.

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
        Dictionary with information about latest delta version.

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
    # TODO pydoc
    latest_delta_version = get_latest_delta_version_details(spark=spark, location=location)
    return latest_delta_version["version"] if latest_delta_version else None

