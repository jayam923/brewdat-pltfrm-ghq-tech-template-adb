import great_expectations as ge
from great_expectations.core import ExpectationValidationResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from typing import Any

from .import common_utils


class DataQualityCheck():
    """Helper class that provides data quality checks for given DataFrame.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to write.

    Returns
    -------
    SparkDFDataset
        SparkDFDataset object for great expectation.
    """

    def __init__(self, df: DataFrame, dbutils: object, spark: SparkSession):
        self.df=df
        self.validator = ge.dataset.SparkDFDataset(df)
        self.result_list = []
        self.dbutils = dbutils
        self.spark = spark

    def get_wider_dq_results(self,
                             ) -> DataFrame:
        """Create function to return dq check results in dataframe
        """
        try:
            result_schema = (
                 StructType(fields=[StructField('function_name', StringType()),
                                    StructField('result_value', StringType()),
                                    StructField('percentage', StringType()),
                                    StructField('dq_rane', StringType()),
                                    StructField('result', StringType()),
                                    StructField('comments', StringType())]
                
                 ))
            
            result_df = self.spark.createDataFrame(data=[*set(self.result_list)], schema=result_schema)
            return result_df
            

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def __get_delta_tables_history_dataframe(self,
                                             target_location: str,
                                             older_version: int,
                                             latest_version: int) -> DataFrame:
        """ Create function to get the hitory and latest version of given table location.
        Parameters
        ----------
        latest_version:int
            deltalake latest version number.
        older_version : int
            deltalake older version number.
        Returns
        -------
        DataFrame
            Dataframe with older version of data of given target location.
            Dataframe with latest version of data of given target location.
        """
        try:
            latest_df = self.spark.read.format("delta").option("versionAsOf", latest_version).load(target_location)
            history_df = self.spark.read.format("delta").option("versionAsOf", older_version).load(target_location)
            return latest_df, history_df

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def __get_result_list(
            self,
            result: ExpectationValidationResult,
            dq_result=None,
            result_value=None,
            dq_function_name=None,
            dq_column_name=None,
            dq_mostly=None,
            dq_range=None,
            dq_comments=None,
        ) -> list:
        """ Create a list for the results from the DQ checks.

        Parameters
        ----------
        result : object
            A ExpectationValidationResult object.
        dq_name : DataFrame
            Name of the DQ check.
        dq_result : BaseDataContext
            Result from the DQ check.
        dq_row_count : DataFrame
            count of records from the DQ check.

        Returns
        -------
        List
            It appends Data quality check results into list

        """
        dq_result = str(result['success'])
        if dq_function_name == "dq_count_of_records_in_table" or dq_function_name == "dq_column_sum_value":
            result_value = str(result['result']['observed_value'])
            dq_range = f" range : [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"
            dq_comments = f" '{dq_column_name} ' : records_count :-> {result['result']['observed_value']}, and range value :-> [{result['expectation_config']['kwargs']['min_value']}, {result['expectation_config']['kwargs']['max_value']}]"

        else:
            dq_mostly = result['expectation_config']['kwargs']['mostly']
            if dq_function_name == "dq_count_for_unique_values_in_compond_columns" or dq_function_name == 'dq_count_for_unique_values_in_columns':
                result_value = str(
                    result['result']['element_count'] - result['result']['unexpected_count'] - result['result'][
                        'missing_count'])
                dq_comments = f" '{dq_column_name} ': total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']} , null_record_count :-> {result['result']['missing_count']}"
                if float(str(int(result_value) / result['result']['element_count'])[:4]) >= dq_mostly:
                    dq_result = 'True'

                else:
                    dq_result = 'False'

            else:
                result_value = str(result['result']['element_count'] - result['result']['unexpected_count'])
                dq_comments = f" '{dq_column_name} ': total_records_count :-> {result['result']['element_count']} , unexpected_record_count :-> {result['result']['unexpected_count']}"

        self.result_list.append(
            (
                dq_function_name,
                result_value,
                dq_mostly,
                dq_range,
                dq_result,
                dq_comments
            )
        )

    def dq_validate_compond_column_unique_values(
               self,
               col_list: list,
               mostly: float,
        ) -> ExpectationValidationResult:
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_list : list
            hold list of result for result list function.
        mostly   : float
            must be a float between 0 and 1. evaluates it as a percentage to fail or pass the validation.

        Returns
        -------
        result
            ExpectationValidationResult object.
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")

            if not col_list:
                raise ValueError("Given list is empty, Please enter valid values")

            col_names = ","
            result = self.validator.expect_compound_columns_to_be_unique(
                col_list,
                mostly,
                result_format="SUMMARY"
            )
            col_names = col_names.join(col_list)
            self.__get_result_list(
                result=result,
                dq_column_name=col_names,
                dq_function_name="dq_count_for_unique_values_in_compond_columns",
                dq_mostly=mostly
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_row_count(
                self,
                min_value: int,
                max_value: int,
        ) -> ExpectationValidationResult:
        """Create function to Assert Assert row count.
        Parameters
        ----------
        min_value : int
            count of the row in the table.
        max_value : int
            count of the row in the table.

        Returns
        -------
        result
            ExpectationValidationResult object.
        """
        try:
            result = self.validator.expect_table_row_count_to_be_between(
                min_value,
                max_value,
                result_format="SUMMARY")
            self.__get_result_list(
                result=result,
                dq_function_name="dq_count_of_records_in_table"
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_column_values_to_not_be_null(self,
                                                 col_name: str,
                                                 mostly: float,
                                                 ) -> ExpectationValidationResult:
        """Create function to check null percentage for given column
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        mostly :float
            thrashold value to validate the test cases.

        Returns
        -------
        result
            ExpectationValidationResult object.
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")

            result = self.validator.expect_column_values_to_not_be_null(
                col_name,
                mostly,
                result_format="SUMMARY"
            )
            self.__get_result_list(
                result=result,
                dq_column_name=col_name,
                dq_function_name="dq_not_null_records_percentage_for_column"
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_range_for_numeric_column_sum_values(
                self,
                col_name: str,
                min_value: int,
                max_value: int,
        ) -> ExpectationValidationResult:
        """Create function to check sum of given numeric column
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        min_value : int
            minumium sum of the column.
        max_value : int
            maximum sum of the column.

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            result = self.validator.expect_column_sum_to_be_between(
                col_name,
                min_value,
                max_value,
                result_format="SUMMARY"
            )
            return result
            self.__get_result_list(
                result=result,
                dq_column_name=col_name,
                dq_function_name="dq_column_sum_value"
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_column_unique_values(
                self,
                col_name: str,
                mostly: int,
        ) -> ExpectationValidationResult:
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        mostly :int
            thrashold value to validate the test cases.

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")

            result = self.validator.expect_column_values_to_be_unique(
                col_name,
                mostly,
                result_format="SUMMARY"
            )
            self.__get_result_list(
                result=result,
                dq_column_name=col_name,
                dq_function_name="dq_count_for_unique_values_in_columns"
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def dq_validate_count_variation_from_previous_version_values(
                     self,
                     target_location: str,
                     older_version: int,
                     latest_version: int
      ) -> ExpectationValidationResult:
        """Create function to check count variation from older version.
        Parameters
        ----------
        min_value : int
            count of the row in the table.
        max_value : int
            count of the row in the table.
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        older_version : int
            Given target delta location of older version.
        latest_version : int
            Given target delta location of latest version.

        Returns
        -------
        result
            ExpectationValidationResult object
        """
        try:
            dq_range = None
            dq_mostly = None
            current_df, history_df = self.__get_delta_tables_history_dataframe(
                target_location=target_location,
                older_version=older_version,
                latest_version=latest_version
            )
            history_load_count = history_df.count()
            Latest_load_count = current_df.count()
            dq_result = str(result['success'])
            dq_function_name = "dq_validate_count_variation_from_previous_version_values"
            result_value = Latest_load_count - history_load_count
            dq_comments = f" total_record_count_in_history :-> {history_load_count} , total_record_count_in_Latest :-> {Latest_load_count}"
  
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

    def dq_validate_null_percentage_variation_from_previous_version_values(
                      self,
                      target_location: str,
                      col_name: str,
                      mostly: float,
                      older_version: int,
                      latest_version: int
     ) -> ExpectationValidationResult:

        """Create function to check null percentage variation with older version file for given column name
        Parameters
        ----------
        target_location : str
            Absolute Delta Lake path for the physical location of this delta table.
        results: object
            A DeltaTable object.
        col_name : str
            Name of the column on which.
        mostly  : float
            threshold value to validate the test cases.

        Returns
        -------
        result
            ExpectationValidationResult object.
        """
        try:
            if mostly < 0.1 or mostly > 1:
                raise ValueError("Invalid expected percentage value , Enter value between the range of 0.1 to 1")

            latest_df, history_df = self.__get_delta_tables_history_dataframe(
                target_location=target_location,
                older_version=older_version,
                latest_version=latest_version
            )
            history_validator = ge.dataset.SparkDFDataset(history_df)
            latest_validator = ge.dataset.SparkDFDataset(latest_df)
            history_result = history_validator.expect_column_values_to_not_be_null(col_name, result_format="SUMMARY")
            current_result = latest_validator.expect_column_values_to_not_be_null(col_name, result_format="SUMMARY")
            dq_result = str(current_result['success'])
            result_value = format( (current_result['result']['unexpected_percent'] - history_result['result']['unexpected_percent']), 'f')
            dq_function_name = "dq_validate_null_percentage_variation_values"
            dq_comments = f" '{col_name}' : null percentage in history :-> {format( (history_result['result']['unexpected_percent']),'f')} , null percentage in latest :-> {format((current_result['result']['unexpected_percent']),'f')}"
            dq_range = None
            result_percentage = current_result['result']['unexpected_percent'] - history_result['result']['unexpected_percent']

            if mostly < result_percentage:
                dq_result = 'False'
            else:
                dq_result = 'True'
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
            
    def check_bad_records_percentage(
        self,
        mostly,  
    )->"DataQualityChecker":
        """Create function to return the bad percentage from given dataframe"""
        try:
            dq_result = None
            dq_comments=None
            dq_range=None
            mostly=mostly
            total_count=self.df.count()
            bad_records_count=self.df.where(F.col('__data_quality_issues').isNull()).count()
            bad_percentage=round((bad_records_count/total_count),2)
            if bad_percentage > mostly:
                dq_result = 'False'
                dq_comments = f" Failed due to bad percentage value :-> {bad_percentage} exceeds the given value :-> {mostly}"
            else:
                dq_result = 'True'
            
            dq_function_name='check_bad_records_percentage'
            result_value=bad_percentage
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
            
    def check_numeric_sum_varation_with_prev(
                      self,
                      target_location: str,
                      col_name: str,
                      min_value: Any,
                      max_value: Any,
                      older_version: int,
                      latest_version: int  
    )->"DataQualityChecker":
        """Create function to return the bad percentage from given dataframe"""
        try:
            dq_function_name = 'check_numeric_sum_varation_with_prev'
            mostly = None
            dq_range = None
            dq_range = f' range : [{min_value}, {max_value}]'
            latest_df, history_df = self.__get_delta_tables_history_dataframe(
                target_location=target_location,
                older_version=older_version,
                latest_version=latest_version
            )
            history_sum_validator = ge.dataset.SparkDFDataset(history_df)
            latest_sum_validator = ge.dataset.SparkDFDataset(latest_df)
            history_result = history_sum_validator.expect_column_sum_to_be_between(col_name, result_format="SUMMARY")
            current_result = latest_sum_validator.expect_column_sum_to_be_between(col_name, result_format="SUMMARY")
            result_value = current_result['result']['observed_value'] - history_result['result']['observed_value']
            dq_comments = f" '{col_name}' : Sum value in history :-> {  history_result['result']['observed_value']} , Sum value in latest :-> { current_result['result']['observed_value']}"           
            if (result_value > min_value) and (result_value < min_value):
                dq_result = 'True'
            else:
                dq_result = 'False'
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
