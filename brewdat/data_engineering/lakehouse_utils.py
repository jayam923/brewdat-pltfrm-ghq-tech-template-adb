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

        # Check that all parameters are valid
        assert_valid_zone(target_zone)
        assert_valid_business_domain(target_business_domain)
        assert_valid_folder_name(source_system)
        assert_valid_folder_name(table_name)

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

        # Check that all parameters are valid
        assert_valid_zone(target_zone)
        assert_valid_business_domain(target_business_domain)
        assert_valid_folder_name(source_system)
        assert_valid_folder_name(table_name)

        return f"{lakehouse_silver_root}/data/{target_zone}/{target_business_domain}/{source_system}/{table_name}".lower()

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def generate_gold_table_location(
    dbutils: object,
    lakehouse_gold_root: str,
    target_zone: str,
    target_business_domain: str,
    data_product: str,
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
    data_product : str
        Data product of the target dataset.
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
        params_list = [lakehouse_gold_root, target_zone, target_business_domain, data_product, database_name, table_name]
        if any(x is None or len(x) == 0 for x in params_list):
            raise ValueError("Location would contain null or empty values.")

        # Check that all parameters are valid
        assert_valid_zone(target_zone)
        assert_valid_business_domain(target_business_domain)
        assert_valid_folder_name(data_product)
        assert_valid_folder_name(database_name)
        assert_valid_folder_name(table_name)

        return f"{lakehouse_gold_root}/data/{target_zone}/{target_business_domain}/{data_product}/{database_name}/{table_name}".lower()

    except Exception:
        common_utils.exit_with_last_exception(dbutils)


def assert_valid_zone(zone):
    """Assert that given zone is valid.

    Valid zones include: afr, apac, eur, ghq, maz, naz, saz.

    Parameters
    ----------
    zone : str
        Zone of the target dataset.
    """
    valid_zones = ["afr", "apac", "eur", "ghq", "maz", "naz", "saz"]
    if zone not in valid_zones:
        raise ValueError(
            f"Invalid value for zone: {zone}."
            f" Must be one of: {', '.join(valid_zones)}"
        )


def assert_valid_business_domain(business_domain):
    """Assert that given business domain is valid.

    Valid business domains include: compliance, finance, marketing, people, sales, supply, tech.

    Parameters
    ----------
    business_domain : str
        Business domain of the target dataset.
    """
    valid_domains = ["compliance", "finance", "marketing", "people", "sales", "supply", "tech"]
    if business_domain not in valid_domains:
        raise ValueError(
            f"Invalid value for business domain: {business_domain}."
            f" Must be one of: {', '.join(valid_domains)}"
        )


def assert_valid_folder_name(folder_name):
    """Assert that given folder name is valid.

    Folder names must start with an alphanumeric character, and must only
    contain alphanumeric characters, dash (-), dot (.), or underscore (_).

    Parameters
    ----------
    folder_name : str
        Name of a folder in the target dataset's path.
    """
    if not re.fullmatch("[a-zA-Z0-9][a-zA-Z0-9-._]*", folder_name):
        raise ValueError(
            f"Invalid value for folder name: {folder_name}."
            " Must start with an alphanumeric character and must only contain"
            " alphanumeric characters, dash (-), dot (.), or underscore (_)"
        )
