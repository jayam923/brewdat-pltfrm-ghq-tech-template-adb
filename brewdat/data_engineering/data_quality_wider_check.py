from typing import Any, List

import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.core import ExpectationValidationResult

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

import pyspark.sql.functions as F

from . import common_utils


class DataQualityChecker:
    """Helper class that provides data quality checks for data in given Delta Lake location.
    Attributes
    ----------
    location : str
        A Delta Lake location.
    spark: SparkSession, default = None
        A Spark session.
    dbutils : Any, default = None
        A Databricks utils object.
    """
    def __init__(
            self,
            location: str,
            spark: SparkSession = None,
            dbutils: Any = None,
    ):
        self.location = location
        self.result_list = []
        self.dbutils = dbutils or globals().get("dbutils")
        self.spark = spark if spark else SparkSession.getActiveSession()
        self.df = spark.read.format("delta").load(location)
        self.validator = ge.dataset.SparkDFDataset(self.df)
        
    def build(self) -> DataFrame:
        """Obtain the resulting DataFrame with data quality checks records.

        Returns
        -------
        DataFrame
            A PySpark DataFrame with data quality metrics.
        """
        try:
            result_schema = (
                StructType(fields=[StructField('validation_rule', StringType()),
                                    StructField('columns', StringType()),
                                    StructField('passed', BooleanType()),
                                    StructField('comments', StringType())])
                )

            return (
                self.spark.createDataFrame(set(self.result_list), result_schema)
                .withColumn("__data_quality_check_ts",F.to_timestamp( F.current_timestamp(),"MM-dd-yyyy HH mm ss SSS"))
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def __append_results(
            self,
            validation_rule: str,
            columns: str,
            passed: bool,
            comments: str,
    ):
        """Appends data quality check results to results list.

        Parameters
        ----------
        validation_rule: str
            Validation rule name.
        columns: List[str]
            Lit of columns observed by validation rule.
        passed: bool
            Whether the validation passed or failed.
        comments: str
            Textual information about failed validation rule.
        """
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
    ) -> "DataQualityChecker":
        """Create function to Assert if column has unique values.

        Parameters
        ----------
        col_list : list
            hold list of result for result list function.
        mostly   : float
            must be a float between 0 and 1. evaluates it as a percentage to fail or pass the validation.
            
        Returns
        -------
        DataQualityChecker
            DataQualityCheck object
        """
        try:
            if not col_list:
                raise ValueError("Given list is empty, Please enter valid values")
                
            result = self.validator.expect_compound_columns_to_be_unique(
                column_list=col_list,
                mostly=mostly,
                result_format="SUMMARY"
            )
            
            unique_values = result['result']['element_count'] - result['result']['unexpected_count'] - result['result']['missing_count']
            unexpected_percent = unique_values / result['result']['element_count']

            if unexpected_percent >= mostly:
                passed = True
                comment = f"Check is 'success' due to {round(float(format(unexpected_percent*100,'f')),2)}% of the records being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."
            else:
                passed = False
                comment = f"Check is 'failed' due to {round(float(format(unexpected_percent*100,'f')),2)}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."
            

            self.__append_results(
                validation_rule="check_compound_column_uniqueness",
                passed=passed,
                columns=', '.join(col_list),
                comments=comment
            )
            #return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_row_count(
             self,
             min_value: int,
             max_value: int,
                        ) -> "DataQualityChecker":
        """Create function to Assert Assert row count.
        Parameters
        ----------
        min_value : int
            minimum threshold value to validate the test cases.
        max_value : int
            maximum threshold value to validate the test cases.
            
        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:
            if min_value > max_value:
                raise ValueError("Minimum value must be less than or equal to Maximum value.")

            result = self.validator.expect_table_row_count_to_be_between(
                min_value=min_value,
                max_value=max_value,
                result_format="SUMMARY")

            if result['success']:
                passed = True
                comment = f"Expected row count to be between {min_value} and {max_value}. " \
                        f"Observed count was {result['result']['observed_value']}."
            else:
                passed = False
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

    def check_column_nulls(
            self,
            col_name: str,
            mostly: float,
    ) -> "DataQualityChecker":
        """Create function to check null percentage for given column.
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        mostly :float
            threshold value to validate the test cases.
            
        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:              
            result = self.validator.expect_column_values_to_not_be_null(
                column=col_name,
                mostly=mostly,
                result_format="SUMMARY"
            )

            passed = result['success']

            if result['result']['unexpected_percent'] <= mostly:
                passed = True
                comment = f"Check is 'success' due to {round(float(format(result['result']['unexpected_percent'],'f')),2)}% of the records being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."
            else:
                passed = False
                comment = f"Check is 'failed' due to {round(float(format(result['result']['unexpected_percent'],'f')),2)}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."

            self.__append_results(
                validation_rule="check_column_nulls",
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
    ) -> "DataQualityChecker":
        """Create function to check sum of given numeric column.
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        min_value : int
            minimum threshold value to validate the test cases.
        max_value : int
            maximum threshold value to validate the test cases.

        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:
            if min_value > max_value:
                raise ValueError("Minimum value must be less than or equal to Maximum value.")
                
            result = self.validator.expect_column_sum_to_be_between(
                column=col_name,
                min_value=min_value,
                max_value=max_value,
                result_format="SUMMARY"
            )

            passed = result['success']

            comment = None
            if not passed:
                comment = f"Check is 'failed' due to Expected column sum to be between {min_value} and {max_value}. " \
                        f"Observed sum was {result['result']['observed_value']}."
            else:
                comment = f"Check is 'success' due to Expected column sum to be between {min_value} and {max_value}. " \
                        f"Observed sum was {result['result']['observed_value']}."

            self.__append_results(
                validation_rule="check_column_sum",
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
    ) -> "DataQualityChecker":
        """Create function to Assert if column has unique values.
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        mostly :int
            threshold value to validate the test cases.

        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:
            result = self.validator.expect_column_values_to_be_unique(
                column=col_name,
                mostly=mostly,
                result_format="SUMMARY"
            )
            unique_values = result['result']['element_count'] - result['result']['unexpected_count'] - result['result']['missing_count']
            unexpected_percent = unique_values / result['result']['element_count']

            if unexpected_percent >= mostly:
                passed = True
                comment = f"Check is 'success' due to {round(float(format(unexpected_percent*100,'f')),2)}% of the records not being " \
                          f"compliant to validation rule. Expected {mostly * 100}% of records to be compliant."
            else:
                passed = False
                comment = f"Check is 'failed' due to {round(float(format(unexpected_percent*100,'f')),2)}% of the records not being " \
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
            min_value: int,
            max_value: int,
            previous_version: int,
            current_version: int,
    ) -> "DataQualityChecker":
        """Create function to check count variation from older version
        Parameters
        ----------
        min_value : int
            minimum threshold value to validate the test cases.
        max_value: int
            maximum threshold value to validate the test cases.
        previous_version : int
            Given target delta location of previos version.
        current_version : int
            Given target delta location of current version.

        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:
            if min_value > max_value:
                raise ValueError("Minimum variation must be less than or equal to Maximum variation.")
                
            previous_count = (
                self.spark.read.format("delta")
                    .option("versionAsOf", previous_version)
                    .load(self.location)
                    .count()
            )
            current_count = (
                self.spark.read.format("delta")
                    .option("versionAsOf", current_version)
                    .load(self.location)
                    .count()
            )
            
            count_diff = (current_count - previous_count)
            count_variation = count_diff / previous_count

            if (min_value <= count_diff) and (count_diff <= max_value):
                passed = True
                comment = f"Check is 'passed' due to record count difference from previous version is {count_diff} and count variation is {round(float(format(count_variation,'f')),2)}%, which is " \
                        f"expected range of {min_value} to {max_value}."
            else:
                passed = False
                comment = f"Check is 'failed' due to record count difference from previous version is {count_diff} and count variation is {round(float(format(count_variation,'f')),2)}%, which is outside of" \
                        f" expected range of {min_value} to {max_value}."
                

            self.__append_results(
                validation_rule="check_count_variation_from_previous_version",
                passed=passed,
                columns=None,
                comments=comment
            )

            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_null_percentage_variation_from_previous_version(
            self,
            col_name: str,
            max_accepted_variation: float,
            previous_version: int,
            current_version: int,
    ) -> ExpectationValidationResult:

        """Create function to check null percentage variation with older version file for given column name.
        Parameters
        ----------
        col_name : str
            Name of the column on which.
        max_accepted_variation  : float
            maximum threshold value to validate the test cases.
        previous_version : int
            Given target delta location of previos version.
        current_version : int
            Given target delta location of current version.
        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:

            previous_validator = ge.dataset.SparkDFDataset(
                self.spark.read.format("delta")
                    .option("versionAsOf", previous_version)
                    .load(self.location)
            )
            current_validator = ge.dataset.SparkDFDataset(
                self.spark.read.format("delta")
                    .option("versionAsOf", current_version)
                    .load(self.location)
            )

            current_result = current_validator.expect_column_values_to_not_be_null(col_name, result_format="SUMMARY")
            previous_result = previous_validator.expect_column_values_to_not_be_null(col_name, result_format="SUMMARY")
            variation =  ((current_result['result']['unexpected_percent']) - (previous_result['result']['unexpected_percent']))

            if variation <= max_accepted_variation:
                passed = True
                comment = f"Check is 'success' due to the percentage of null records for column '{col_name}' increased by {round(float(format(variation,'f')),2)}% " \
                          f"(version {current_version}) when compared with previous version (version {previous_version}) of the table, which is" \
                          f" lesser than the max allowed of {max_accepted_variation * 100}% , previous version : {round(float(format(current_result['result']['unexpected_percent'],'f')),2)}%."\
                          f" current version : {round(float(format(previous_result['result']['unexpected_percent'],'f')),2)}%"  
            else:
                passed = False
                comment = f"Check is 'failed' due to the percentage of null records for column '{col_name}' decreased by {round(float(format(variation,'f')),2)}% " \
                          f"(version {current_version}) when compared with previous version (version {previous_version}) of the table, which is" \
                          f" higher than the max allowed of {max_accepted_variation * 100}% , previous version : {round(float(format(current_result['result']['unexpected_percent'],'f')),2)}%."\
                          f" current version : {round(float(format(previous_result['result']['unexpected_percent'],'f')),2)}%"            

            self.__append_results(
                validation_rule="check_null_percentage_variation_from_previous_version",
                passed=passed,
                columns=col_name,
                comments=comment
            )

            return self
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)
            
    def check_bad_records_percentage(
            self,
            min_percentage: float,
            max_percentage: float,
            current_version: int,
    )->"DataQualityChecker":
        """Create function to return the bad percentage from given dataframe.
        Parameters
        ----------
        max_accepted_variation  : float
            maximum threshold value to validate the test cases.
        max_accepted_variation  : float
            minimum threshold value to validate the test cases.
        current_version : int
            Given target delta location of current version.
        
        Returns
        -------
        DataQualityChecker
            ExpectationValidationResult object.
        """
        try:
            if min_percentage > max_percentage:
                raise ValueError("Minimum percentage must be less than or equal to Maximum percentage.")
                
            current_df = (
                self.spark.read.format("delta")
                    .option("versionAsOf", current_version)
                    .load(self.location)
            )
            
            total_count=current_df.count()
            bad_records_count=current_df.where(F.col('__data_quality_issues').isNotNull()).count()
            bad_percentage=(bad_records_count/total_count)
            
            if (min_percentage <= bad_percentage) and (bad_percentage <= max_percentage):
                passed = True
                comment = f" Check is 'passed' due to bad percentage value {round(float(format(bad_percentage*100,'f')),2)}%, which is between the expected range of {min_percentage*100}% to {max_percentage*100}%."

            else:
                passed = False
                comment = f" Check is 'failed' due to bad percentage value {round(float(format(bad_percentage*100,'f')),2)}%, which is not in between the expected range of {min_percentage*100}% to {max_percentage*100}%"
                
            self.__append_results(
                validation_rule="check_bad_records_percentage",
                passed=passed,
                columns=None,
                comments=comment
            )
            
        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)

    def check_numeric_sum_varation_from_previous_version(
            self,
            col_name: str,
            min_value: Any,
            max_value: Any,
            previous_version: int,
            current_version: int  
    )->"DataQualityChecker":
        """Create function to return sum variation from previous to latest dataframe for given column.
            Parameters
            ----------
            col_name : str
                Name of the column on which.
            max_accepted_variation  : float
                maximum threshold value to validate the test cases.
            max_accepted_variation  : float
                minimum threshold value to validate the test cases.
            previous_version : int
                Given target delta location of previos version.
            current_version : int
                Given target delta location of current version.
        
            Returns
            -------
            DataQualityChecker
                ExpectationValidationResult object.
        """
        try:
            if min_value > max_value:
                raise ValueError("Minimum length must be less than or equal to maximum length.")
                
            previous_validator = ge.dataset.SparkDFDataset(
                self.spark.read.format("delta")
                    .option("versionAsOf", previous_version)
                    .load(self.location)
            )
            current_validator = ge.dataset.SparkDFDataset(
                self.spark.read.format("delta")
                    .option("versionAsOf", current_version)
                    .load(self.location)
            )
            
            history_result = previous_validator.expect_column_sum_to_be_between(col_name, result_format="SUMMARY")
            current_result = current_validator.expect_column_sum_to_be_between(col_name, result_format="SUMMARY")
            sum_diff = current_result['result']['observed_value'] - history_result['result']['observed_value']
            
            if ( min_value <= sum_diff) and (sum_diff <= max_value):
                passed = True
                comment =  f" Check is 'passed' due to sum diffrence of '{col_name}' value is {sum_diff}, which is between than the given values {[min_value,max_value]}"
            else:
                passed = False
                comment =  f" Check is 'failed' due to sum diffrence of '{col_name}' value is {sum_diff}, which is not between than the given values {[min_value,max_value]}"
                
            self.__append_results(
                validation_rule="check_numeric_sum_varation_from_previous_version",
                passed=passed,
                columns=col_name,
                comments=comment
            )

        except Exception:
            common_utils.exit_with_last_exception(self.dbutils)
            
    @classmethod
    def check_foreign_key_column_exits(
                    cls,
                    dim_df: DataFrame,
                    fact_df: DataFrame,
                    primary_key: str,
                    foreign_key: str,
                    dbutils: object,
    )->"DataQualityChecker":
        """Create function to return the bad percentage from given dataframe.
          Parameters
          ----------
            dim_df : DataFrame
                PySpark DataFrame to validate.
            fact_df  : DataFrame
                PySpark DataFrame to validate..
            primary_key  : str
                primery key of table.
            foreign_key : str
                Forgien key of table.
            dbutils : int
                A Databricks utils object.
                
          Returns
            -------
            DataQualityChecker
                result value.
        """
        try:
            dim_df = dim_df.select(primary_key)
            fact_df = fact_df.select(foreign_key)
            result_df = fact_df.join(dim_df,fact_df[foreign_key]==dim_df[primary_key],how='left')
            result_value = result_df.where(F.col(primary_key).isNull()).select(foreign_key).distinct().count()
            return result_value
        
        except Exception:
            common_utils.exit_with_last_exception(dbutils)