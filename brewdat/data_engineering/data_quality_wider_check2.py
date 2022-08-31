from typing import Any

import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core import ExpectationValidationResult

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType

from . import common_utils
from . import read_utils
from .read_utils import RawFileFormat


class DataQualityCheck:
    """Helper class that provides data quality checks for given DataFrame.
    
    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to write.
        
    Returns
    -------
    SparkDFDataset
        SparkDFDataset object for great expectation
    """
    def __init__(
            self,
            location: str,
            spark: SparkSession = SparkSession.getActiveSession(),
            dbutils: Any = None,
    ):
        self.location = location
        self.result_list = []
        self.dbutils = dbutils or globals().get("dbutils")
        self.spark = spark
        self.df = spark.read.format("delta").load(location)
        self.validator = ge.dataset.SparkDFDataset(self.df)
        
    def build(self) -> DataFrame:
        """Create function to return dq check results in dataframe
        """
        try:
            result_schema = (
                 StructType(fields=[StructField('validation_rule', StringType()),
                                    StructField('columns', StringType()),
                                    StructField('passed', BooleanType()),
                                    StructField('comments', StringType())])
                 )                   
            result_df\
                = self.spark.createDataFrame([*set(self.result_list)], result_schema)
            return result_df
        
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)


    # TODO remove this function
    def __get_delta_tables_history_dataframe(self,
        target_location: str, 
        older_version : int,
        latest_version : int )-> DataFrame:
        """ Create function to get the hitory and latest version of given table location
        Parameters
        ----------
        latest_version:int
            deltalake latest version number
        older_version : int
            deltalake older version number
        Returns
        -------
        DataFrame
            Dataframe with older version of data of given target location
            Dataframe with latest version of data of given target location
        """
        try:
            latest_df = self.spark.read.format("delta").option("versionAsOf", latest_version).load(target_location)
            history_df = self.spark.read.format("delta").option("versionAsOf", older_version).load(target_location)
            return latest_df, history_df

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)
            
    # def __append_results(self,
    #                      result: ExpectationValidationResult,
    #                      dq_function_name: str = None,
    #                      dq_column_name: str = None,
    #                      dq_mostly: str = None,
    #                      dq_range: str = None, ):
    #     """ Create a list for the results from the DQ checks.
    #
    #     Parameters
    #     ----------
    #     result : object
    #         A ExpectationValidationResult object.
    #     dq_name : DataFrame
    #         Name of the DQ check.
    #     dq_result : BaseDataContext
    #         Result from the DQ check.
    #     dq_row_count : DataFrame
    #         count of records from the DQ check.
    #
    #     Returns
    #     -------
    #     List
    #         It appends Data quality check results into list
    #
    #     """
    #     dq_result = bool(result['success'])
    #     if dq_function_name == "dq_count_of_records_in_table" or dq_function_name == "dq_column_sum_value" :
    #         result_value = str(result['result']['observed_value'])
    #         dq_range = f" range : [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
    #         dq_comments = f" '{dq_column_name} ' : records_count :-> {result['result']['observed_value']}, and range value :-> [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
    #
    #     else:
    #         dq_mostly=result['expectation_config']['kwargs']['mostly']
    #         if dq_function_name == "dq_count_for_unique_values_in_compond_columns" or dq_function_name == 'dq_count_for_unique_values_in_columns':
    #             result_value = str(result['result']['element_count'] - result['result']['unexpected_count'] - result['result']['missing_count'])
    #             dq_comments = f" '{dq_column_name} ': total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']} , null_record_count :-> {result['result']['missing_count']}"
    #             if float(str(int(result_value)/result['result']['element_count'])[:4]) >= dq_mostly:
    #                 dq_result = True
    #
    #             else:
    #                 dq_result = False
    #
    #         else:
    #             result_value = str(result['result']['element_count'] - result['result']['unexpected_count'])
    #             dq_comments = f" '{dq_column_name} ': total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']}"
    #
    #     self.result_list.append(
    #             (
    #       dq_function_name,
    #       result_value,
    #       dq_mostly,
    #       dq_range,
    #       dq_result,
    #       dq_comments
    #             )
    #     )

    def __append_results(
            self,
            validation_rule: str,
            columns: str,
            passed: bool,
            comments: str,
    ):
        self.result_list.append(
            (
                validation_rule,
                columns,
                passed,
                comments,
            )
        )
        
    def check_compound_column_uniqueness(
            self,
            col_list: list,
            mostly: float,
    ) -> "DataQualityCheck":
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_list : list
            hold list of result for result list function
        mostly   : float
            must be a float between 0 and 1. evaluates it as a percentage to fail or pass the validation
            
        Returns
        -------
        DataQualityCheck
            DataQualityCheck object
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
              
            if not col_list:
                raise ValueError("Given list is empty, Please enter valid values")
                
            result = self.validator.expect_compound_columns_to_be_unique(
                column_list=col_list,
                mostly=mostly,
                result_format="SUMMARY"
            )

            passed = result['success']

            comment = None
            if not passed:
                comment = f"Check failed due to {result['result']['unexpected_percent']}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."

            col_names = ",".join(col_list)

            self.__append_results(
                validation_rule="check_compound_column_uniqueness",
                passed=result['success'],
                columns=col_names,
                comments=comment
            )
            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_row_count(self,
                        min_value: int,
                        max_value: int,
                        ) -> "DataQualityCheck":
        """Create function to Assert Assert row count.
        Parameters
        ----------
        min_value : int
            count of the row in the table
        max_value : int
            count of the row in the table
            
        Returns
        -------
        DataQualityCheck
            ExpectationValidationResult object
        """
        try:

            result = self.validator.expect_table_row_count_to_be_between(
                min_value=min_value,
                max_value=max_value,
                result_format="SUMMARY")
            passed = result['success']
            comment = None
            if not passed:
                comment = f"Expected row count to be between {min_value} and {max_value}. " \
                          f"Observed count was {result['result']['observed_value']}."

            self.__append_results(
                validation_rule="check_row_count",
                passed=passed,
                columns=None,
                comments=comment
            )
            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_nulls(
            self,
            col_name: str,
            mostly: float,
    ) -> "DataQualityCheck":
        """Create function to check null percentage for given column
        Parameters
        ----------
        col_name : str
            Name of the column on which
        mostly :float
            thrashold value to validate the test cases
            
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
                
            result = self.validator.expect_column_values_to_not_be_null(
                column=col_name,
                mostly=mostly,
                result_format="SUMMARY"
            )

            passed = result['success']

            comment = None
            if not passed:
                comment = f"Check failed due to {result['result']['unexpected_percent']}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."

            self.__append_results(
                validation_rule="check_nulls",
                passed=passed,
                columns=col_name,
                comments=comment
            )

            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_column_sum(
            self,
            col_name: str,
            min_value: float,
            max_value: float,
    ) -> "DataQualityCheck":
        """Create function to check sum of given numeric column
        Parameters
        ----------
        col_name : str
            Name of the column on which
        min_value : int
            minimum sum of the column
        max_value : int
            maximum sum of the column

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            # TODO validate column type
            result = self.validator.expect_column_sum_to_be_between(
                column=col_name,
                min_value=min_value,
                max_value=max_value,
                result_format="SUMMARY"
            )

            passed = result['success']

            comment = None
            if not passed:
                comment = f"Expected row sum to be between {min_value} and {max_value}. " \
                          f"Observed sum was {result['result']['observed_value']}."

            self.__append_results(
                validation_rule="check_row_count",
                passed=passed,
                columns=col_name,
                comments=comment
            )

            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)
            
    def check_column_uniqueness(
            self,
            col_name: str,
            mostly: int,
    ) -> "DataQualityCheck":
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_name : str
            Name of the column on which
        mostly :int
            threshold value to validate the test cases

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")

            result = self.validator.expect_column_values_to_be_unique(
                column=col_name,
                mostly=mostly,
                result_format="SUMMARY"
            )
            passed = result['success']

            comment = None
            if not passed:
                comment = f"Check failed due to {result['result']['unexpected_percent']}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."

            self.__append_results(
                validation_rule="check_column_uniqueness",
                passed=passed,
                columns=col_name,
                comments=comment
            )

            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_count_variation_from_previous_version(
            self,
            target_location: str,
            min_value: int,
            max_value: int,
            older_version: int,
            latest_version: int
    ) -> "DataQualityCheck":
        """Create function to check count variation from older version
        Parameters
        ----------
        min_value : int
            count of the row in the table
        max_value : int
            count of the row in the table
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        older_version : int
            Given target delta location of older version
        latest_version : int
            Given target delta location of latest version

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            current_df = read_utils.read_raw_dataframe(
                spark=self.spark,
                dbutils=self.dbutils,
                file_format=RawFileFormat.DELTA,
                location=target_location,
                delta_version_as_of=latest_version
            )

            history_df = read_utils.read_raw_dataframe(
                spark=self.spark,
                dbutils=self.dbutils,
                file_format=RawFileFormat.DELTA,
                location=target_location,
                delta_version_as_of=older_version
            )

            history_load_count = history_df.count()
            Latest_validator = ge.dataset.SparkDFDataset(current_df)
            result = Latest_validator.expect_table_row_count_to_be_between(
                min_value,
                max_value,
                result_format = "SUMMARY"
            )
            dq_result = str(result['success'])
            dq_function_name = "dq_validate_count_variation_from_previous_version_values"
            result_value = result['result']['observed_value'] - history_load_count
            dq_comments = f" total_record_count_in_history :-> {history_load_count} , total_record_count_in_Latest :-> {result['result']['observed_value']}"
            dq_range = f" range : [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
            dq_mostly = None 
            if result['expectation_config']['kwargs']['min_value'] <= result_value <= result['expectation_config']['kwargs']['max_value']:
                dq_result = "True"
            self.result_list.append(
                (
                dq_function_name,
                result_value, 
                dq_mostly, 
                dq_range, 
                dq_result,
                dq_comments)
            )  
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_null_percentage_variation_from_previous_version_values(self,
        target_location: str,
        col_name : str,
        mostly : float,
        older_version : int,
        latest_version : int
        )-> ExpectationValidationResult:

        """Create function to check null percentage variation with older version file for given column name
        Parameters
        ----------
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        results: object
            A DeltaTable object.
        col_name : str
            Name of the column on which 
        mostly  : float
            threshold value to validate the test cases
            
        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly<0.1 or mostly>1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")
                
            dq_min_value = None
            dq_min_value = None
            current_df, history_df = self.__get_delta_tables_history_dataframe(
                target_location = target_location,
                older_version=older_version,
                latest_version=latest_version
            )
            history_result = self.validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
            current_result = self.validator.expect_column_values_to_not_be_null(col_name, result_format = "SUMMARY")
            dq_result = str(current_result['success'])
            result_value = round(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'], 2)
            dq_function_name  = "dq_validate_null_percentage_variation_values"
            dq_comments = f" '{col_name}' : null percentage in history :-> {history_result['result']['unexpected_percent']:.2f} , null percentage in latest :-> {current_result['result']['unexpected_percent']:.2f}"
            dq_range = f' range : [{dq_min_value}, {dq_min_value}]'
            cal_mostly = float(str(current_result['result']['unexpected_percent']- history_result['result']['unexpected_percent'])[:4])
            if(cal_mostly <= mostly):
                dq_result = "True"
            self.result_list.append(
                (
                dq_function_name, 
                result_value, 
                mostly, 
                dq_range, 
                dq_result,
                dq_comments
            )
            )      
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)