import json
import sys
import traceback
from enum import Enum, unique
from typing import TypedDict


@unique
class RunStatus(str, Enum):
    """Available run statuses.

    SUCCEEDED: Represents a succeeded run status.
    FAILED: Represents a failed run status.
    """
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ReturnObject(TypedDict):
    """Object that holds metadata from a data write operation.

    Attributes
    ----------
    status : str
        Resulting status for this write operation.
    target_object : str
        Target object that we intended to write to.
    num_records_read : int
        Number of records read from the DataFrame.
    num_records_loaded : int
        Number of records written to the target table.
    error_message : str
        Error message describing whichever error that occurred.
    error_details : str
        Detailed error message or stack trace for the above error.
    """
    status: str
    target_object: str
    num_records_read: int
    num_records_loaded: int
    num_records_errored_out: int
    error_message: str
    error_details: str


class DataFrameCommon: 
    """Reusable functions for all BrewDat projects.

    Attributes
    ----------
    dbutils : object
        A Databricks utils object.
    """

    def __init__(self, dbutils: object):
        self.dbutils = dbutils


    def exit_with_object(self, results: ReturnObject):
        """Finish execution returning an object to the notebook's caller.

        Used to return the results of a write operation to the orchestrator.

        Parameters
        ----------
        results : ReturnObject
            Object containing the results of a write operation.
        """
        self.dbutils.notebook.exit(json.dumps(results))


    def exit_with_last_exception(self):
        """Handle the last unhandled exception, returning an object to the notebook's caller.

        The most recent exception is obtained from sys.exc_info().

        Examples
        --------
        >>> try:
        >>>    # some code
        >>> except:
        >>>    BrewDatLibrary.exit_with_last_exception()
        """
        exc_type, exc_value, _ = sys.exc_info()
        results = self._build_return_object(
            status=self.RunStatus.FAILED,
            target_object=None,
            error_message=f"{exc_type.__name__}: {exc_value}",
            error_details=traceback.format_exc(),
        )
        self.exit_with_object(results)
