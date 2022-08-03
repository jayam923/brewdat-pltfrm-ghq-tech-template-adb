import json
import sys
import traceback
from enum import Enum, unique
from typing import List

from pyspark.sql import SparkSession


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
    error_message : str, default=""
        Error message describing whichever error that occurred.
    error_details : str, default=""
        Detailed error message or stack trace for the above error.
    """
    def __init__(
        self,
        status: RunStatus,
        target_object: str,
        num_records_read: int = 0,
        num_records_loaded: int = 0,
        error_message: str = "",
        error_details: str = ""
    ):
        self.status = status
        self.target_object = target_object
        self.num_records_read = num_records_read
        self.num_records_loaded = num_records_loaded
        self.num_records_errored_out = num_records_read - num_records_loaded
        self.error_message = error_message[:8000]
        self.error_details = error_details


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
    >>>    common_utils..exit_with_last_exception()
    """
    exc_type, exc_value, _ = sys.exc_info()
    results = ReturnObject(
        status=RunStatus.FAILED,
        target_object=None,
        error_message=f"{exc_type.__name__}: {exc_value}",
        error_details=traceback.format_exc(),
    )
    exit_with_object(dbutils, results)


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

    We use Hadoop Configuration to make it available to the RDD API.
    This is a requirement for using spark-xml and similar libraries.

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

    except Exception:
        exit_with_last_exception(dbutils)
