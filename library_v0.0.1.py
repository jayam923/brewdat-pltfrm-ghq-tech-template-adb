# Databricks notebook source
class Framework:
    """Reusable functions for all BrewDat projects.

    Methods
    -------
    read_raw_dataframe -> DataFrame
        Read a DataFrame from the Raw Layer.
    clean_column_names -> DataFrame
        Normalize the name of all the columns in a given DataFrame.
    create_or_replace_business_key_column -> DataFrame
        Create a standard business key concatenating multiple columns.
    create_or_replace_audit_columns -> DataFrame
        Create or replace BrewDat audit columns in the given DataFrame.
    deduplicate_records -> DataFrame
        Deduplicate rows from a DataFrame using key and watermark columns.
    drop_empty_columns -> DataFrame
        Drop columns which are null or empty for all the rows in the DataFrame.
    generate_bronze_table_location -> str
        Build the standard location for a Bronze table.
    generate_silver_table_location -> str
        Build the standard location for a Silver table.
    generate_gold_table_location -> str
        Build the standard location for a Gold table.
    write_delta_table -> ReturnObject
        Write the DataFrame as a delta table.
    exit_with_object
        Finish execution returning an object to the notebook's caller.
    exit_with_last_exception
        Handle the last unhandled exception, returning an object to the notebook's caller.
    """

    import json
    import pyspark.sql.functions as F
    import re
    import sys
    import traceback
    from datetime import datetime
    from delta.tables import DeltaTable
    from enum import Enum, unique
    from pyspark.sql import DataFrame
    from pyspark.sql.window import Window
    from typing import List, TypedDict

    ########################################
    # Constants, enums, and helper classes #
    ########################################

    # TODO: move these somewhere else
    ENV = check_workspace_env()
    LAKEHOUSE_LANDING_ROOT = f"abfss://raw@brewdatbeesrawbrz{ENV[0]}.dfs.core.windows.net" 
    LAKEHOUSE_BRONZE_ROOT = f"abfss://bronze@brewdatbeesrawbrz{ENV[0]}.dfs.core.windows.net"
    LAKEHOUSE_SILVER_ROOT = f"abfss://silver@brewdatbeesslvgld{ENV[0]}.dfs.core.windows.net"
    LAKEHOUSE_GOLD_ROOT = f"abfss://gold@brewdatbeesslvgld{ENV[0]}.dfs.core.windows.net"


    @unique
    class LoadType(str, Enum):
        OVERWRITE_TABLE = "OVERWRITE_TABLE"
        OVERWRITE_PARTITION = "OVERWRITE_PARTITION"
        APPEND_ALL = "APPEND_ALL"  # only for Bronze tables, as it is bad for backfilling
        APPEND_NEW = "APPEND_NEW"
        UPSERT = "UPSERT"
        TYPE_2_SCD = "TYPE_2_SCD"  # not yet implemented


    @unique
    class RawFileFormat(str, Enum):
        PARQUET = "PARQUET"
        DELTA = "DELTA"
        ORC = "ORC"
        CSV = "CSV"


    @unique
    class SchemaEvolutionMode(str, Enum):
        FAIL_ON_SCHEMA_MISMATCH = "FAIL_ON_SCHEMA_MISMATCH"
        ADD_NEW_COLUMNS = "ADD_NEW_COLUMNS"
        IGNORE_NEW_COLUMNS = "IGNORE_NEW_COLUMNS"
        OVERWRITE_SCHEMA = "OVERWRITE_SCHEMA"
        RESCUE_NEW_COLUMNS = "RESCUE_NEW_COLUMNS"  # not yet implemented


    @unique
    class RunStatus(str, Enum):
        SUCCEEDED = "SUCCEEDED"
        FAILED = "FAILED"


    class ReturnObject(TypedDict):
        status: str
        target_object: str
        num_records_read: int
        num_records_loaded: int
        num_records_errored_out: int
        error_message: str
        error_details: str



    ##################
    # Public methods #
    ##################

    @classmethod
    def read_raw_dataframe(
        cls,
        file_format: RawFileFormat,
        location: str,
    ) -> DataFrame:
        """Read a DataFrame from the Raw Layer. Convert all data types to string.

        Parameters
        ----------
        file_format : RawFileFormat
            The raw file format use in this dataset (CSV, PARQUET, etc.).
        location : str
            Absolute Data Lake path for the physical location of this dataset.

        Returns
        -------
        DataFrame
            The PySpark DataFrame read from the Raw Layer.
        """
        try:
            df = (
                spark.read
                .format(file_format.lower())
                .option("header", True)
                .option("escape", "\"")
                .option("mergeSchema", True)
                .load(location)
            )

            # Cast everything to string
            if file_format != cls.RawFileFormat.CSV:
                non_string_columns = [col for col, dtype in df.dtypes if dtype != "string"]
                for column in non_string_columns:
                    df = df.withColumn(column, F.col(column).cast("string"))

            return df

        except:
            cls.exit_with_last_exception()


    @classmethod
    def clean_column_names(
        cls,
        df: DataFrame,
        except_for: List[str] = [],
    ) -> DataFrame:
        """Normalize the name of all the columns in a given DataFrame.

        Uses BrewDat's standard approach as seen in other Notebooks.
        Improved to also trim (strip) whitespaces.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        except_for : List[str], default=[]
            A list of column names that should NOT be modified.

        Returns
        -------
        DataFrame
            The modified PySpark DataFrame with renamed columns.
        """
        try:
            column_names = df.schema.names
            for column_name in column_names:
                if column_name in except_for:
                    continue  # Skip

                # \W is "anything that is not alphanumeric or underscore"
                # Equivalent to [^A-Za-z0-9_]
                new_column_name = re.sub("\W+", "_", column_name.strip())
                if column_name != new_column_name:
                    df = df.withColumnRenamed(column_name, new_column_name)
            return df

        except:
            cls.exit_with_last_exception()


    @classmethod
    def create_or_replace_business_key_column(
        df: DataFrame,
        business_key_column_name: str,
        key_columns: List[str],
        separator: str = "__",
    ) -> DataFrame:
        """Create a standard business key concatenating multiple columns.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        business_key_column_name : str
            The name of the concatenated business key column.
        key_columns : List[str]
            The names of the columns used to uniquely identify each record the table.
        separator : str, default="__"
            A string to separate the values of each column in the business key.

        Returns
        -------
        DataFrame
            The PySpark DataFrame with the desired business key.
        """
        try:
            if not key_columns:
                raise ValueError("No key column was given")

            # Check for NULL values
            filter_clauses = [f"`{key_column}` IS NULL" for key_column in key_columns]
            filter_string = " OR ".join(filter_clauses)
            if df.filter(filter_string).limit(1).count() > 0:
                # TODO: improve error message
                raise ValueError("Business key would contain null values.")

            df = df.withColumn(business_key_column_name, F.lower(F.concat_ws("__", *key_columns)))

            return df

        except:
            cls.exit_with_last_exception()


    @classmethod
    def create_or_replace_audit_columns(cls, df: DataFrame) -> DataFrame:
        """Create or replace BrewDat audit columns in the given DataFrame.

        The following audit columns are created/replaced:
            _insert_gmt_ts: timestamp of when the record was inserted.
            _update_gmt_ts: timestamp of when the record was last updated.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.

        Returns
        -------
        DataFrame
            The modified PySpark DataFrame with audit columns.
        """
        try:
            # Get current timestamp
            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Create or replace columns
            if "_insert_gmt_ts" in df.columns:
                df = df.fillna(current_timestamp, "_insert_gmt_ts")
            else:
                df = df.withColumn("_insert_gmt_ts", F.lit(current_timestamp).cast("timestamp"))
            df = df.withColumn("_update_gmt_ts", F.lit(current_timestamp).cast("timestamp"))
            return df

        except:
            cls.exit_with_last_exception()


    @classmethod
    def deduplicate_records(
        cls,
        df: DataFrame,
        key_columns: List[str],
        watermark_column: str,
    ) -> DataFrame:
        """Deduplicate rows from a DataFrame using key and watermark columns.

        We do not use orderBy followed by dropDuplicates because it
        would require a coalesce(1) to preserve the order of the rows.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        key_columns : List[str]
            The names of the columns used to uniquely identify each record the table.
        watermark_column : str
            The name of a datetime column used to select the newest records.

        Returns
        -------
        DataFrame
            The deduplicated PySpark DataFrame.
        """
        try:
            if not key_columns:
                raise ValueError("No key column was given")

            return (
                df
                .withColumn("__dedup_row_number", F.row_number().over(
                    Window.partitionBy(*key_columns).orderBy(F.col(watermark_column).desc())
                ))
                .filter("__dedup_row_number = 1")
                .drop("__dedup_row_number")
            )

        except:
            cls.exit_with_last_exception()


    @classmethod
    def drop_empty_columns(
        cls,
        df: DataFrame,
        except_for: List[str] = [],
    ) -> DataFrame:
        """Drop columns which are null or empty for all the rows in the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to modify.
        except_for : List[str], default=[]
            A list of column names that should NOT be dropped.

        Returns
        -------
        DataFrame
            The modified PySpark DataFrame.
        """
        try:
            non_empty_counts = df.selectExpr([
                f"COUNT(`{col}`) AS `{col}`" if datatype != "string"
                else f"COUNT(NULLIF(NULLIF(NULLIF(NULLIF(`{col}`, ''), ' '), 'null'), 'NULL')) AS `{col}`"
                for col, datatype in df.dtypes
                if col not in except_for
            ]).first().asDict()
            null_columns = [col for col, count in non_empty_counts.items() if count == 0]
            df = df.drop(*null_columns)
            return df

        except:
            cls.exit_with_last_exception()


    @classmethod
    def generate_bronze_table_location(
        cls,
        source_zone: str,
        source_business_domain: str,
        source_system_name: str,
        source_dataset: str,
    ) -> str:
        """Build the standard location for a Bronze table.

        Parameters
        ----------
        source_zone : str
            Zone of the source dataset.
        source_business_domain : str
            Business domain of the source dataset.
        source_system_name : str
            Name of the source system.
        source_dataset : str
            Name of the source dataset.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [source_zone, source_business_domain, source_system_name, source_dataset]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            location = f"{cls.LAKEHOUSE_BRONZE_ROOT}/data/{source_zone}/{source_business_domain}/{source_system_name}/{source_dataset}"
            return location.lower()

        except:
            cls.exit_with_last_exception()


    @classmethod
    def generate_silver_table_location(
        cls,
        source_zone: str,
        source_business_domain: str,
        source_system_name: str,
        table_name: str,
    ) -> str:
        """Build the standard location for a Silver table.

        Parameters
        ----------
        source_zone : str
            Zone of the source system.
        source_business_domain : str
            Business domain of the source system.
        source_system_name : str
            Name of the source system.
        table_name : str
            Name of the target table in the metastore.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [source_zone, source_business_domain, source_system_name, table_name]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            location = f"{cls.LAKEHOUSE_SILVER_ROOT}/data/{source_zone}/{source_business_domain}/{source_system_name}/{table_name}"
            return location.lower()

        except:
            cls.exit_with_last_exception()


    @classmethod
    def generate_gold_table_location(
        cls,
        target_zone: str,
        target_business_domain: str,
        project: str,
        schema_name: str,
        table_name: str,
    ) -> str:
        """Build the standard location for a Gold table.

        Parameters
        ----------
        target_zone : str
            Zone of the resulting dataset.
        target_business_domain : str
            Business domain of the resulting dataset.
        project : str
            Project of the resulting dataset.
        schema_name : str
            Name of the target schema/database for the table in the metastore.
        table_name : str
            Name of the target table in the metastore.

        Returns
        -------
        str
            Standard location for the delta table.
        """
        try:
            # Check that no parameter is None or empty string
            params_list = [target_zone, target_business_domain, project, schema_name, table_name]
            if any(x is None or len(x) == 0 for x in params_list):
                raise ValueError("Location would contain null or empty values.")

            location = f"{cls.LAKEHOUSE_GOLD_ROOT}/data/{target_zone}/{target_business_domain}/{project}/{schema_name}/{table_name}"
            return location.lower()

        except:
            cls.exit_with_last_exception()


    @classmethod
    def write_delta_table(
        cls,
        df: DataFrame,
        location: str,
        schema_name: str,
        table_name: str,
        load_type: LoadType,
        key_columns: List[str] = [],
        partition_columns: List[str] = [],
        schema_evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.ADD_NEW_COLUMNS,
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
        load_type : Framework.LoadType
            Specifies the way in which the table should be loaded.
            OVERWRITE_TABLE: the entire table is rewritten in every execution.
                Avoid whenever possible, as this is not good for large tables.
                This deletes records that are not present in df.
            OVERWRITE_PARTITION: overwrite a single partition based on partitionColumns.
                This deletes records that are not present in df for the chosen partition.
                The df must be filtered such that it contains a single partition.
            APPEND_NEW: write new records in the df to the existing table.
                Records for which the key already exists in the table are ignored.
            UPSERT: write new records and update existing records based on the key.
                This does NOT delete existing records that are not included in df.
            TYPE_2_SCD: use the standard type-2 Slowly Changing Dimension implementation.
                This essentially uses an upsert that keeps track of all previous versions of each record.
                For more information: https://en.wikipedia.org/wiki/Slowly_changing_dimension
        key_columns : List[str], default=[]
            The names of the columns used to uniquely identify each record the table.
            Used for APPEND_NEW, UPSERT, and TYPE_2_SCD load types.
        partition_columns : List[str], default=[]
            The names of the columns used to partition the table.
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution

        Returns
        -------
        ReturnObject
            Object containing the results of a write operation.
        """
        num_records_read = 0
        num_records_loaded = 0

        try:
            # Count source records
            num_records_read = df.count()

            # Table must exist if we are merging data
            if load_type != cls.LoadType.APPEND_ALL and not DeltaTable.isDeltaTable(spark, location):
                print("Delta table does not exist yet. Setting load_type to APPEND_ALL for this run.")
                load_type = cls.LoadType.APPEND_ALL

            # Use optimized writes to create less small files
            spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)

            # Set load_type options
            if load_type == cls.LoadType.OVERWRITE_TABLE:
                if num_records_read == 0:
                    raise ValueError("Attempted to overwrite a table with an empty dataset. Operation aborted.")

                cls._write_table_using_overwrite_table(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == cls.LoadType.OVERWRITE_PARTITION:
                if not partition_columns:
                    raise ValueError("No partition column was given")

                if num_records_read == 0:
                    raise ValueError("Attempted to overwrite a partition with an empty dataset. Operation aborted.")

                cls._write_table_using_overwrite_partition(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == cls.LoadType.APPEND_ALL:
                cls._write_table_using_append_all(
                    df=df,
                    location=location,
                    partition_columns=partition_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == cls.LoadType.APPEND_NEW:
                if not key_columns:
                    raise ValueError("No key column was given")

                cls._write_table_using_append_new(
                    df=df,
                    location=location,
                    key_columns=key_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == cls.LoadType.UPSERT:
                if not key_columns:
                    raise ValueError("No key column was given")

                cls._write_table_using_upsert(
                    df=df,
                    location=location,
                    key_columns=key_columns,
                    schema_evolution_mode=schema_evolution_mode,
                )
            elif load_type == cls.LoadType.TYPE_2_SCD:
                if not key_columns:
                    raise ValueError("No key column was given")

                raise NotImplementedError
            else:
                raise NotImplementedError

            # Find out how many records we have just written
            num_version_written = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
            delta_table = DeltaTable.forPath(spark, location)
            history_df = (
                delta_table.history()
                .filter(f"version = {num_version_written}")
                .select(F.col("operationMetrics.numOutputRows").cast("int"))
            )
            num_records_loaded = history_df.first()[0]

            # Create the Hive database and table
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name};")
            spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} USING DELTA LOCATION '{location}';")

            return cls._build_return_object(
                status=cls.RunStatus.SUCCEEDED,
                target_object=f"{schema_name}.{table_name}",
                num_records_read=num_records_read,
                num_records_loaded=num_records_loaded,
            )

        except Exception as e:
            return cls._build_return_object(
                status=cls.RunStatus.FAILED,
                target_object=f"{schema_name}.{table_name}",
                num_records_read=num_records_read,
                num_records_loaded=num_records_loaded,
                error_message=str(e),
                error_details=traceback.format_exc(),
            )


    @classmethod
    def exit_with_object(cls, results: ReturnObject):
        """Finish execution returning an object to the notebook's caller.

        Used to return the results of a write operation to the orchestrator.

        Parameters
        ----------
        results : ReturnObject
            Object containing the results of a write operation.
        """
        dbutils.notebook.exit(json.dumps(results))


    @classmethod
    def exit_with_last_exception(cls):
        """Handle the last unhandled exception, returning an object to the notebook's caller.

            The most recent exception is obtained from sys.exc_info().

            Examples
            --------
            try:
                # some code
            except:
                Framework.exit_with_last_exception()
        """
        exc_type, exc_value, _ = sys.exc_info()
        results = cls._build_return_object(
            status=cls.RunStatus.FAILED,
            target_object=None,
            error_message=f"{exc_type.__name__}: {exc_value}",
            error_details=traceback.format_exc(),
        )
        cls.exit_with_object(results)



    ###################
    # Private methods #
    ###################

    @classmethod
    def _write_table_using_overwrite_table(
        cls,
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
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
        """
        df_writer = (
            df.write
            .format("delta")
            .mode("overwrite")
        )

        # Set partition options
        if partition_columns:
            df_writer = df_writer.partitionBy(partition_columns)

        # Set schema_evolution_mode options
        if schema_evolution_mode == cls.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == cls.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(spark, location):
                table_columns = DeltaTable.forPath(spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    @classmethod
    def _write_table_using_overwrite_partition(
        cls,
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
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
        """
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
        if schema_evolution_mode == cls.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == cls.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(spark, location):
                table_columns = DeltaTable.forPath(spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    @classmethod
    def _write_table_using_append_all(
        cls,
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
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
        """
        df_writer = (
            df.write
            .format("delta")
            .mode("append")
        )

        # Set partition options
        if partition_columns:
            df_writer = df_writer.partitionBy(partition_columns)

        # Set schema_evolution_mode options
        if schema_evolution_mode == cls.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == cls.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            df_writer = df_writer.option("mergeSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(spark, location):
                table_columns = DeltaTable.forPath(spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            df_writer = df_writer.option("overwriteSchema", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
            raise NotImplementedError
        else:
            raise NotImplementedError

        # Write to the delta table
        df_writer.save(location)


    @classmethod
    def _write_table_using_append_new(
        cls,
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
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
        """
        # Set schema_evolution_mode options
        if schema_evolution_mode == cls.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == cls.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            original_auto_merge = spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(spark, location):
                table_columns = DeltaTable.forPath(spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            raise ValueError("OVERWRITE_SCHEMA is not supported in APPEND_NEW load type")
        elif schema_evolution_mode == cls.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
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

        # Reset spark.conf
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", original_auto_merge)


    @classmethod
    def _write_table_using_upsert(
        cls,
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
        schema_evolution_mode : Framework.SchemaEvolutionMode, default=ADD_NEW_COLUMNS
            Specifies the way in which schema mismatches should be handled.
            FAIL_ON_SCHEMA_MISMATCH: fail if the table's schema is not compatible with the DataFrame's.
                This is the default Spark behavior when no option is given.
            ADD_NEW_COLUMNS: schema evolution through adding new columns to the target table.
                This is the same as using the option "mergeSchema".
            IGNORE_NEW_COLUMNS: drop DataFrame columns that do not exist in the table's schema.
                Does nothing if the table does not yet exist in the Hive metastore.
            OVERWRITE_SCHEMA: overwrite the table's schema with the DataFrame's schema.
                This is the same as using the option "overwriteSchema".
            RESCUE_NEW_COLUMNS: Create a new struct-type column to collect data for new columns.
                This is the same strategy used in AutoLoader's rescue mode.
                For more information: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html#schema-evolution
        """
        # Set schema_evolution_mode options
        if schema_evolution_mode == cls.SchemaEvolutionMode.FAIL_ON_SCHEMA_MISMATCH:
            pass
        elif schema_evolution_mode == cls.SchemaEvolutionMode.ADD_NEW_COLUMNS:
            original_auto_merge = spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.IGNORE_NEW_COLUMNS:
            if DeltaTable.isDeltaTable(spark, location):
                table_columns = DeltaTable.forPath(spark, location).columns
                new_df_columns = [col for col in df.columns if col not in table_columns]
                df = df.drop(*new_df_columns)
        elif schema_evolution_mode == cls.SchemaEvolutionMode.OVERWRITE_SCHEMA:
            raise ValueError("OVERWRITE_SCHEMA is not supported in UPSERT load type")
        elif schema_evolution_mode == cls.SchemaEvolutionMode.RESCUE_NEW_COLUMNS:
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

        # Reset spark.conf
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", original_auto_merge)


    @classmethod
    def _build_return_object(
        cls,
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
        status : Framework.RunStatus
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