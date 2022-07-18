import datetime
from pyspark.sql import DataFrame
import great_expectations as ge
from typing import Tuple
from ruamel import yaml
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults
from . import common_utils, transform_utils, read_utils

    
def read_raw_Datafiles(file_path):
    df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(file_path)
    return df

def configure_data_context(    
        dbutils: object,
        df: DataFrame
        ) -> Tuple[BaseDataContext, RuntimeBatchRequest, Validator]:
    try:
        root_loc ="/dbfs/FileStore/dataquality/"
        data_context_config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory= root_loc),)
        context = BaseDataContext(project_config=data_context_config)
        my_spark_datasource_config = {
        "name": "brewdat_datasource_name",
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"},
        "data_connectors": {
            "Testing_dataconnector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": [
                    "some_key_maybe_pipeline_stage",
                    "some_other_key_maybe_run_id",
                    ],
                }
            },
        }
        context.add_datasource(**my_spark_datasource_config)
        batch_request = RuntimeBatchRequest(
        datasource_name = "brewdat_datasource_name",
        data_connector_name="Testing_dataconnector",
        data_asset_name="testing_dataset",  # This can be anything that identifies this data_asset for you
        batch_identifiers={
            "some_key_maybe_pipeline_stage": "pipeline_layer",
            "some_other_key_maybe_run_id": f"Data_Quality_Results_{datetime.date.today().strftime('%Y%m%d')}",
        },
        runtime_parameters={"batch_data": df},
        )
        expectation_suite_name = "expectation_suite_name"
        context.create_expectation_suite(
        expectation_suite_name = expectation_suite_name, overwrite_existing=True)
        validator = context.get_validator(
        batch_request = batch_request,
        expectation_suite_name = expectation_suite_name)
        return context, batch_request, validator
    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def dq_validate_column_type(
    dbutils: object,
    col_name: str, 
    col_type: str,
    validator: Validator):
    try:
        result = validator.expect_column_values_to_be_of_type(col_name, col_type, result_format = "SUMMARY")
        print(result)
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def dq_validate_column_nulls_values(dbutils: object, validator: Validator, col_name):
    try:
        validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def dq_validate_column_unique_values(dbutils: object, validator: Validator, col_name):
    try:
        validator.expect_column_values_to_be_unique(col_name, result_format = "SUMMARY")
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def dq_validate_column_range_values(dbutils: object, validator: Validator, col_name, min_value, max_value):
    try:
        validator.expect_column_values_to_be_between(col_name, min_value, max_value, result_format = "SUMMARY")
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        
def dq_validate_column_lengths(dbutils: object, validator: Validator, col_name, col_len):
    try:
        validator.expect_column_value_lengths_to_equal(col_name, col_len, result_format = "SUMMARY")
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def dq_validate_row_count(dbutils: object, validator: Validator,row_len):
    try:
        validator.expect_table_row_count_to_equal(row_len, result_format = "SUMMARY") 
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def dq_validate_column_exist(dbutils: object, validator: Validator,col_name):
    try:
        validator.expect_column_to_exist(col_name, result_format = "SUMMARY")
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def save_expectation_suite_in_validator():
    try:
        test_config=validator.get_expectation_suite()
        validator.save_expectation_suite(discard_failed_expectations=False)
    except Exception:
        common_utils.exit_with_last_exception(dbutils)

def get_dq_checkpoint_result():
    try:
        checkpoint_config = {
        "name": "check_point_name",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        }
        context.add_checkpoint(**checkpoint_config)
        checkpoint_result = context.run_checkpoint(
        checkpoint_name="check_point_name",
        validations=[
                    {
                "batch_request": batch_request,
                "expectation_suite_name": "expectation_suite_name",
                    }
                ],
             )
        return checkpoint_result
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
