import json
import sys
import traceback
from enum import Enum, unique


@unique
class RunStatus(str, Enum):
    """Available run statuses.

    SUCCEEDED: Represents a succeeded run status.
    FAILED: Represents a failed run status.
    """
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


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
        error_details: str = "",
    ):
        self.status = status
        self.target_object = target_object
        self.num_records_read = num_records_read
        self.num_records_loaded = num_records_loaded
        self.num_records_errored_out = num_records_read - num_records_loaded
        self.error_message = error_message[:8000]
        self.error_details = error_details
        
    def result(self):
        dct={'status':self.status,'target_object':self.target_object,'num_records_read':self.num_records_read,'num_records_loaded':self.num_records_loaded,'num_records_errored_out':self.num_records_errored_out,'error_message':self.error_message,'error_details':self.error_details}
        return dct


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
    dbutils.notebook.exit(results_json)


def exit_with_last_exception():
    """Handle the last unhandled exception, returning an object to the notebook's caller.

    The most recent exception is obtained from sys.exc_info().

    Examples
    --------
    >>> try:
    >>>    # some code
    >>> except:
    >>>    exit_with_last_exception()
    """
    exc_type, exc_value, _ = sys.exc_info()
    results = ReturnObject(
        status=RunStatus.FAILED,
        target_object=None,
        error_message=f"{exc_type.__name__}: {exc_value}",
        error_details=traceback.format_exc(),
    )
    exit_with_object(results)
