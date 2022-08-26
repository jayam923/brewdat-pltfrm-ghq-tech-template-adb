import json
import sys
import traceback
from enum import Enum, unique
from typing import List

from py4j.protocol import Py4JError
from pyspark.sql import DataFrame, SparkSession


@unique
class RunStatus(str, Enum):
    """Available run statuses.
    """
    SUCCEEDED = "SUCCEEDED"
    """Represents a succeeded run status."""
    FAILED = "FAILED"
    """Represents a failed run status."""


class ReturnObject():
    """Object that holds metadata from a data write operation.

    Attributes
    ----------
    status : RunStatus
        Resulting status for this write operation.
    target_object : str
        Target object that we intended to write to.
    num_records_read : int, default=0
        Number of records read from the DataFrame.
    num_records_loaded : int, default=0
        Number of records written to the target table.
    num_records_errored_out: int, default=0
        Number of records that have been rejected.
    error_message : str, default=""
        Error message describing whichever error that occurred.
    error_details : str, default=""
        Detailed error message or stack trace for the above error.
    old_version_number: int, default=None
        Version number of target object before write operation.
    new_version_number: int, default=None
        Version number of target object after write operation.
    effective_data_interval_start : str, default=""
        The effective watermark lower bound of the input DataFrame.
    effective_data_interval_end : str, default=""
        The effective watermark upper bound of the input DataFrame.
    """
    def __init__(
        self,
        status: RunStatus,
        target_object: str,
        num_records_read: int = 0,
        num_records_loaded: int = 0,
        num_records_errored_out: int = 0,
        error_message: str = "",
        error_details: str = "",
        old_version_number: int = None,
        new_version_number: int = None,
        effective_data_interval_start: str = "",
        effective_data_interval_end: str = "",
    ):
        self.status = status
        self.target_object = target_object
        self.num_records_read = num_records_read
        self.num_records_loaded = num_records_loaded
        self.num_records_errored_out = num_records_errored_out
        self.error_message = error_message[:8000]
        self.error_details = error_details
        self.old_version_number = old_version_number
        self.new_version_number = new_version_number
        self.effective_data_interval_start = effective_data_interval_start
        self.effective_data_interval_end = effective_data_interval_end

    def __str__(self):
        return str(vars(self))


class ColumnMapping():
    """Object the holds the source-to-target-mapping information
    for a single column in a DataFrame.

    Attributes
    ----------
    source_column_name : str
        Column name in the source DataFrame.
    target_data_type : str
        The data type to which input column will be cast to.
    sql_expression : str, default=None
        Spark SQL expression to create the target column.
        If None, simply cast and possibly rename the source column.
    target_column_name : str, default=None
        Column name in the target DataFrame.
        If None, use source_column_name as target_column_name.
    nullable : bool, default=True
        Whether the target column should allow null values.
        Used for data quality checks.
    """
    def __init__(
        self,
        source_column_name: str,
        target_data_type: str,
        sql_expression: str = None,
        target_column_name: str = None,
        nullable: bool = True,
    ):
        self.source_column_name = source_column_name
        self.target_data_type = target_data_type
        self.sql_expression = sql_expression
        self.target_column_name = target_column_name or source_column_name
        self.nullable = nullable

    def __str__(self):
        return str(vars(self))


def configure_spn_access_for_adls(
    spark: SparkSession,
    dbutils: object,
    storage_account_names: List[str],
    key_vault_name: str,
    spn_client_id: str,
    spn_secret_name: str,
    spn_tenant_id: str = "cef04b19-7776-4a94-b89b-375c77a8f936",
):
    """Set up access to an ADLS Storage Account using a Service Principal.

    We try to use Spark Context to make it available to the RDD API.
    This is a requirement for using spark-xml and similar libraries.
    If Spark Context fails, we use Spark Session configuration instead.

    Parameters
    ----------
    spark : SparkSession
        A Spark session.
    dbutils : object
        A Databricks utils object.
    storage_account_names : List[str]
        Name of the ADLS Storage Accounts to configure with this SPN.
    key_vault_name : str
        Databricks secret scope name. Usually the same as the name of the Azure Key Vault.
    spn_client_id : str
        Application (Client) Id for the Service Principal in Azure Active Directory.
    spn_secret_name: str
        Name of the secret containing the Service Principal's client secret.
    spn_tenant_id: str, default="cef04b19-7776-4a94-b89b-375c77a8f936"
        Tenant Id for the Service Principal in Azure Active Directory.
    """
    try:
        for storage_account_name in storage_account_names:
            storage_account_suffix = f"{storage_account_name}.dfs.core.windows.net"
            try:
                spark._jsc.hadoopConfiguration().set(
                    f"fs.azure.account.auth.type.{storage_account_suffix}",
                    "OAuth"
                )
                spark._jsc.hadoopConfiguration().set(
                    f"fs.azure.account.oauth.provider.type.{storage_account_suffix}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
                )
                spark._jsc.hadoopConfiguration().set(
                    f"fs.azure.account.oauth2.client.id.{storage_account_suffix}",
                    spn_client_id
                )
                spark._jsc.hadoopConfiguration().set(
                    f"fs.azure.account.oauth2.client.secret.{storage_account_suffix}",
                    dbutils.secrets.get(key_vault_name, spn_secret_name)
                )
                spark._jsc.hadoopConfiguration().set(
                    f"fs.azure.account.oauth2.client.endpoint.{storage_account_suffix}",
                    f"https://login.microsoftonline.com/{spn_tenant_id}/oauth2/token"
                )
            except Py4JError:
                print("Could not configure ADLS access using Spark Context. " + \
                      "Falling back to Spark Session configuration. " + \
                      "XML and Excel libraries will not be supported.")

                spark.conf.set(
                    f"fs.azure.account.auth.type.{storage_account_suffix}",
                    "OAuth"
                )
                spark.conf.set(
                    f"fs.azure.account.oauth.provider.type.{storage_account_suffix}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
                )
                spark.conf.set(
                    f"fs.azure.account.oauth2.client.id.{storage_account_suffix}",
                    spn_client_id
                )
                spark.conf.set(
                    f"fs.azure.account.oauth2.client.secret.{storage_account_suffix}",
                    dbutils.secrets.get(key_vault_name, spn_secret_name)
                )
                spark.conf.set(
                    f"fs.azure.account.oauth2.client.endpoint.{storage_account_suffix}",
                    f"https://login.microsoftonline.com/{spn_tenant_id}/oauth2/token"
                )
    except Exception:
        exit_with_last_exception(dbutils)


def list_non_metadata_columns(df: DataFrame) -> List[str]:
    """Obtain a list of DataFrame columns except for metadata columns.

    Metadata columns are all columns whose name begins with "__".

    Parameters
    ----------
    df : DataFrame
        The PySpark DataFrame to inspect.

    Returns
    -------
    List[str]
        The list of DataFrame columns, except for metadata columns.
    """
    return [col for col in df.columns if not col.startswith("__")]


def exit_with_object(dbutils: object, results: ReturnObject):
    """Finish execution returning an object to the notebook's caller.

    Used to return the results of a write operation to the orchestrator.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    results : ReturnObject
        Object containing the results of a write operation.
    """
    results_json = json.dumps(results, default=vars)
    if dbutils:
        dbutils.notebook.exit(results_json)
    else:
        raise Exception(results_json)


def exit_with_last_exception(dbutils: object):
    """Handle the last unhandled exception, returning an object to the notebook's caller.

    The most recent exception is obtained from sys.exc_info().

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.

    Examples
    --------
    >>> try:
    >>>    # some code
    >>> except:
    >>>    common_utils.exit_with_last_exception(dbutils)
    """
    exc_type, exc_value, _ = sys.exc_info()
    results = ReturnObject(
        status=RunStatus.FAILED,
        target_object="",
        error_message=f"{exc_type.__name__}: {exc_value}",
        error_details=traceback.format_exc(),
    )
    exit_with_object(dbutils, results)
