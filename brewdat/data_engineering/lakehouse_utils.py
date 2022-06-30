import re
from . import common_utils


def generate_bronze_table_location(
    dbutils: object,
    lakehouse_bronze_root: str,
    target_zone: str,
    target_business_domain: str,
    source_system: str,
    table_name: str,
) -> str:
    """Build the standard location for a Bronze table.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    lakehouse_bronze_root : str
        Root path to the Lakehouse's Bronze layer.
        Format: "abfss://bronze@storage_account.dfs.core.windows.net".
        Value varies by environment, so you should use environment variables.
    target_zone : str
        Zone of the target dataset.
    target_business_domain : str
        Business domain of the target dataset.
    source_system : str
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
        params_list = [lakehouse_bronze_root, target_zone, target_business_domain, source_system, table_name]
        if any(x is None or len(x) == 0 for x in params_list):
            raise ValueError("Location would contain null or empty values.")
        
        check_zone(target_zone)
        check_domain(target_business_domain)
        check_table(table_name)
        
        return f"{lakehouse_bronze_root}/data/{target_zone}/{target_business_domain}/{source_system}/{table_name}".lower()

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def generate_silver_table_location(
    dbutils: object,
    lakehouse_silver_root: str,
    target_zone: str,
    target_business_domain: str,
    source_system: str,
    table_name: str,
) -> str:
    """Build the standard location for a Silver table.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    lakehouse_silver_root : str
        Root path to the Lakehouse's Silver layer.
        Format: "abfss://silver@storage_account.dfs.core.windows.net".
        Value varies by environment, so you should use environment variables.
    target_zone : str
        Zone of the target dataset.
    target_business_domain : str
        Business domain of the target dataset.
    source_system : str
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
        params_list = [lakehouse_silver_root, target_zone, target_business_domain, source_system, table_name]
        if any(x is None or len(x) == 0 for x in params_list):
            raise ValueError("Location would contain null or empty values.")
        
        check_zone(target_zone)
        check_domain(target_business_domain)
        check_table(table_name)

        return f"{lakehouse_silver_root}/data/{target_zone}/{target_business_domain}/{source_system}/{table_name}".lower()

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def generate_gold_table_location(
    dbutils: object,
    lakehouse_gold_root: str,
    target_zone: str,
    target_business_domain: str,
    project: str,
    database_name: str,
    table_name: str,
) -> str:
    """Build the standard location for a Gold table.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    lakehouse_gold_root : str
        Root path to the Lakehouse's Gold layer.
        Format: "abfss://gold@storage_account.dfs.core.windows.net".
        Value varies by environment, so you should use environment variables.
    target_zone : str
        Zone of the target dataset.
    target_business_domain : str
        Business domain of the target dataset.
    project : str
        Project of the target dataset.
    database_name : str
        Name of the target database for the table in the metastore.
    table_name : str
        Name of the target table in the metastore.

    Returns
    -------
    str
        Standard location for the delta table.
    """
    try:
        # Check that no parameter is None or empty string
        params_list = [lakehouse_gold_root, target_zone, target_business_domain, project, database_name, table_name]
        if any(x is None or len(x) == 0 for x in params_list):
            raise ValueError("Location would contain null or empty values.")
            
        check_zone(target_zone)
        check_domain(target_business_domain)
        check_table(table_name)

        return f"{lakehouse_gold_root}/data/{target_zone}/{target_business_domain}/{project}/{database_name}/{table_name}".lower()

    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        

def check_zone(target_zone):
    """Check the standard table_name.

    Parameters
    ----------
    target_zone : str
        Zone of the target dataset.
    """
    expected_zone=["afr", "apac", "eur", "ghq", "maz", "naz", "saz"]
    if target_zone not in expected_zone:
        raise ValueError("Zone does not have expected value.")


def check_domain(target_business_domain):
    """Check the standard table_name.

    Parameters
    ----------
    target_business_domain : str
        Business domain of the target dataset.
    """
    expected_domain=["compliance","finance","marketing","people","sales","supply","tech"]
    if target_business_domain not in expected_domain:
        raise ValueError("Domain does not have expected value.")
        

def check_table(table_name):
    """Check the standard table_name.

    Parameters
    ----------
    table_name : str
        Name of the target table in the metastore.
    """
    if bool(re.match('[a-zA-Z0-9][a-zA-Z0-9\.\_\-]+$', table_name)) !=True:
        raise ValueError("Folder should start with alphanumeric characters.")
        