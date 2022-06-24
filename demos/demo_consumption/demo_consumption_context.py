# Databricks notebook source
import os

# Gather standard Lakehouse environment variables
environment = os.getenv("ENVIRONMENT")
lakehouse_silver_root = os.getenv("LAKEHOUSE_SILVER_ROOT")
lakehouse_gold_root = os.getenv("LAKEHOUSE_GOLD_ROOT")

# Ensure that all standard Lakehouse environment variables are set
if None in [environment, lakehouse_silver_root, lakehouse_gold_root]:
    raise Exception("This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster.")

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
if environment == "dev":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmslvgldd"],
        key_vault_name="brewdatpltfrmghqtechakvd",
        spn_client_id="1d3aebfe-929c-4cc1-a988-31c040d2b798",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-d",
    )
elif environment == "qa":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmslvgldq"],
        key_vault_name="brewdatpltfrmghqtechakvq",
        spn_client_id="12345678-1234-1234-1234-123456789999",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-q",
    )
elif environment == "prod":
    common_utils.configure_spn_access_for_adls(
        spark=spark,
        dbutils=dbutils,
        storage_account_names=["brewdatpltfrmslvgldp"],
        key_vault_name="brewdatpltfrmghqtechakvp",
        spn_client_id="12345678-1234-1234-1234-123456789999",
        spn_secret_name="brewdat-spn-pltfrm-ghq-tech-template-rw-p",
    )
