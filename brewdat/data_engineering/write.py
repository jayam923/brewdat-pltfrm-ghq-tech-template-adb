import json
import os
import re
import sys
import traceback

import pyspark.sql.functions as F

from datetime import datetime
from enum import Enum, unique
from typing import List, TypedDict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from .common import DataFrameCommon, ReturnObject, RunStatus


class DataFrameWrite: 
    """Reusable functions for all BrewDat projects.

    Attributes
    ----------
    spark : SparkSession
        A Spark session.
    """
    
    
    ########################################
    # Constants, enums, and helper classes #
    ########################################


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
   

    ##################
    # Public methods #
    ##################

    
    def __init__(self, spark: SparkSession):
        self.spark = spark
           
    
    def write_delta_table(
        self,
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
        # TODO: refactor this and all related write methods
        num_records_read = 0
        num_records_loaded = 0

        try:
            # Count source records
            num_records_read = df.count()

            # Table must exist if we are merging data
            if load_type != self.LoadType.APPEND_ALL and not DeltaTable.isDeltaTable(self.spark, location):
                print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
                load_type = self.LoadType.APPEND_ALL

            # Use optimized writes to create less small files
            self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
            self.spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", True)

            # Set load_type options
            if load_type == self.LoadType.OVERWRITE_TABLE:
                if num_records_read == 0:
                    raise ValueError("Attempted to overwrite a table with an empty dataset. Operation aborted.")

                self._write_table_using_overwrite_table(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == self.LoadType.OVERWRITE_PARTITION:
                if not partition_columns:
                    raise ValueError("No partition column was given")

                if num_records_read == 0:
                    raise ValueError("Attempted to overwrite a partition with an empty dataset. Operation aborted.")

                self._write_table_using_overwrite_partition(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == self.LoadType.APPEND_ALL:
                self._write_table_using_append_all(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == self.LoadType.APPEND_NEW:
                if not key_columns:
                    raise ValueError("No key column was given")

                self._write_table_using_append_new(
                    df=df,
                    location=location,
                    key_columns=key_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == self.LoadType.UPSERT:
                if not key_columns:
                    raise ValueError("No key column was given")

                self._write_table_using_upsert(
                    df=df,
                    location=location,
                    key_columns=key_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == self.LoadType.TYPE_2_SCD:
                if not key_columns:
                    raise ValueError("No key column was given")

                raise NotImplementedError
            else:
                raise NotImplementedError

            # Find out how many records we have just written
            delta_table = DeltaTable.forPath(self.spark, location)
            history_df = (
                delta_table
                .history(1)
                .select(F.col("operationMetrics.numOutputRows").cast("int"))
            )
            num_records_loaded = history_df.first()[0]

            # Create the Hive database and table
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`;")

            # TODO: drop existing table if its location has changed

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}`
                USING DELTA
                LOCATION '{location}';
            """)

            # Vacuum the delta table
            self.spark.sql(f"""
                ALTER TABLE `{schema_name}`.`{table_name}`
                SET TBLPROPERTIES (
                    'delta.deletedFileRetentionDuration' = 'interval {time_travel_retention_days} days',
                    'delta.logRetentionDuration' = 'interval {time_travel_retention_days} days'
                );
            """)
            self.spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", True)
            self.spark.sql(f"VACUUM `{schema_name}`.`{table_name}`;")

            return self._build_return_object(
                status=RunStatus.SUCCEEDED,
                target_object=f"{schema_name}.{table_name}",
                num_records_read=num_records_read,
                num_records_loaded=num_records_loaded,
            )

        except Exception as e:
            return self._build_return_object(
                status=RunStatus.FAILED,
                target_object=f"{schema_name}.{table_name}",
                num_records_read=num_records_read,
                num_records_loaded=num_records_loaded,
                error_message=str(e),
                error_details=traceback.format_exc(),
            )
            
            
    ###################
    # Private methods #
    ###################

    
    def _write_table_using_overwrite_table(
        self,
        df: DataFrame,
        location: str,
        partition_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    ):
        """Write the DataFrame using OVERWRITE_TABLE.

        Parameters
        ----------
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
        # TODO: refactor this and all related write methods
        df_writer = (
            df.write
            .format("delta")
            .mode("overwrite")
        )

        # Set partition options
        if partition_columns:
            df_writer = df_writer.partitionBy(partition_columns)

        # Set schema_evolution_mode options
        if schema_evolution_mode == self.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == self.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(self.spark, location):
                table_columns = DeltaTable.forPath(self.spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == self.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    def _write_table_using_overwrite_partition(
        self,
        df: DataFrame,
        location: str,
        partition_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    ):
        """Write the DataFrame using OVERWRITE_PARTITION.

        Parameters
        ----------
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
        # TODO: refactor this and all related write methods
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
        if schema_evolution_mode == self.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == self.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(self.spark, location):
                table_columns = DeltaTable.forPath(self.spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == self.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    def _write_table_using_append_all(
        self,
        df: DataFrame,
        location: str,
        partition_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    ):
        """Write the DataFrame using APPEND_ALL.

        Parameters
        ----------
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
        # TODO: refactor this and all related write methods
        df_writer = (
            df.write
            .format("delta")
            .mode("append")
        )

        # Set partition options
        if partition_columns:
            df_writer = df_writer.partitionBy(partition_columns)

        # Set schema_evolution_mode options
        if schema_evolution_mode == self.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == self.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(self.spark, location):
                table_columns = DeltaTable.forPath(self.spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == self.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    @classmethod
    def _write_table_using_append_new(
        self,
        df: DataFrame,
        location: str,
        key_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    ):
        """Write the DataFrame using APPEND_NEW.

        Parameters
        ----------
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
        # TODO: refactor this and all related write methods
        # Set schema_evolution_mode options
        if schema_evolution_mode == self.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == self.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            original_auto_merge = self.spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(self.spark, location):
                table_columns = DeltaTable.forPath(self.spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == self.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            raise ValueError("OVERWRITE_SCHEMA is not supported in APPEND_NEW load type")
        elif schema_evolution_mode == self.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Build merge condition
        merge_condition_parts = [f"source.`{col}` = target.`{col}`" for col in key_columns]
        merge_condition = " AND ".join(merge_condition_parts)

        # Write to the delta table
        delta_table = DeltaTable.forPath(self.spark, location)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Reset spark.conf
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", original_auto_merge)


    def _write_table_using_upsert(
        self,
        df: DataFrame,
        location: str,
        key_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
    ):
        """Write the DataFrame using UPSERT.

        Parameters
        ----------
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
        # TODO: refactor this and all related write methods
        # Set schema_evolution_mode options
        if schema_evolution_mode == self.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == self.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            original_auto_merge = self.spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
        elif schema_evolution_mode == self.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(self.spark, location):
                table_columns = DeltaTable.forPath(self.spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == self.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            raise ValueError("OVERWRITE_SCHEMA is not supported in UPSERT load type")
        elif schema_evolution_mode == self.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Build merge condition
        merge_condition_parts = [f"source.`{col}` = target.`{col}`" for col in key_columns]
        merge_condition = " AND ".join(merge_condition_parts)

        # Write to the delta table
        delta_table = DeltaTable.forPath(self.spark, location)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Reset spark.conf
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", original_auto_merge)

        
    def _build_return_object(
        self,
        status: RunStatus,
        target_object: str,
        num_records_read: int = 0,
        num_records_loaded: int = 0,
        error_message: str = "",
        error_details: str = "",
    ) -> ReturnObject:
        """Build the return object for a write operation.

        Parameters
        ----------
        status : BrewDatLibrary.RunStatus
            Resulting status for this write operation.
        target_object : str
            Target object that we intended to write to.
        num_records_read : int, default=0
            Number of records read from the DataFrame.
        num_records_loaded : int, default=0
            Number of records written to the target table.
        error_message : str, default=""
            Error message describing whichever error that occurred.
        error_details : str, default=""
            Detailed error message or stack trace for the above error.

        Returns
        -------
        ReturnObject
            Object containing the results of a write operation.
        """
        return {
            "status": status,
            "target_object": target_object,
            "num_records_read": num_records_read,
            "num_records_loaded": num_records_loaded,
            "num_records_errored_out": num_records_read - num_records_loaded,
            "error_message": error_message[:8000],
            "error_details": error_details,
        }