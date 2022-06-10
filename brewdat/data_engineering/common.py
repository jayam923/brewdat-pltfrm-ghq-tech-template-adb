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


class ReturnObject(TypedDict):
    """Object that holds metadata from a data write operation.

    Attributes
    ----------
    status : str
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
    status: str
    target_object: str
    num_records_read: int
    num_records_loaded: int
    num_records_errored_out: int
    error_message: str
    error_details: str
    
    

class RunStatus(str, Enum):
    """Object that holds metadata from a data write operation.

        Attributes
        ----------
        status : str
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
    status: str
    target_object: str
    num_records_read: int
    num_records_loaded: int
    num_records_errored_out: int
    error_message: str
    error_details: str
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class DataFrameCommon: 
    """Reusable functions for all BrewDat projects.

    Attributes
    ----------
    spark : SparkSession
        A Spark session.
    """


    def __init__(self, spark: SparkSession, dbutils: object):
        self.spark = spark
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
