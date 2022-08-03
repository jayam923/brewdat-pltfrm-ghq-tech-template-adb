# Databricks notebook source
import os

# Read standard environment variable
environment = os.getenv("ENVIRONMENT")
if environment not in ["dev", "qa", "prod"]:
    raise Exception(
        "This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster."
    )

# COMMAND ----------

# Export variables whose values depend on the environment: dev, qa, or prod
if environment == "dev":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzd"
    print(f"adls_raw_bronze_storage_account_name: {adls_raw_bronze_storage_account_name}")

    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldd"
    print(f"adls_silver_gold_storage_account_name: {adls_silver_gold_storage_account_name}")

    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbdev"
    print(f"adls_brewdat_ghq_storage_account_name: {adls_brewdat_ghq_storage_account_name}")

    key_vault_name = "brewdatpltfrmghqtechakvd"
    print(f"key_vault_name: {key_vault_name}")

    spn_client_id = "1d3aebfe-929c-4cc1-a988-31c040d2b798"
    print(f"spn_client_id: {spn_client_id}")

    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-d"
    print(f"spn_secret_name: {spn_secret_name}")

elif environment == "qa":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzq"
    print(f"adls_raw_bronze_storage_account_name: {adls_raw_bronze_storage_account_name}")

    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldq"
    print(f"adls_silver_gold_storage_account_name: {adls_silver_gold_storage_account_name}")

    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbqa"
    print(f"adls_brewdat_ghq_storage_account_name: {adls_brewdat_ghq_storage_account_name}")

    key_vault_name = "brewdatpltfrmghqtechakvq"
    print(f"key_vault_name: {key_vault_name}")

    spn_client_id = None
    print(f"spn_client_id: {spn_client_id}")

    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-q"
    print(f"spn_secret_name: {spn_secret_name}")
    

elif environment == "prod":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzp"
    print(f"adls_raw_bronze_storage_account_name: {adls_raw_bronze_storage_account_name}")

    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldp"
    print(f"adls_silver_gold_storage_account_name: {adls_silver_gold_storage_account_name}")

    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbprod"
    print(f"adls_brewdat_ghq_storage_account_name: {adls_brewdat_ghq_storage_account_name}")

    key_vault_name = "brewdatpltfrmghqtechakvp"
    print(f"key_vault_name: {key_vault_name}")

    spn_client_id = None
    print(f"spn_client_id: {spn_client_id}")

    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-p"
    print(f"spn_secret_name: {spn_secret_name}")

# COMMAND ----------

# Export additional helper variables
lakehouse_raw_root = f"abfss://raw@{adls_raw_bronze_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_raw_root: {lakehouse_raw_root}")

lakehouse_bronze_root = f"abfss://bronze@{adls_raw_bronze_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_bronze_root: {lakehouse_bronze_root}")

lakehouse_silver_root = f"abfss://silver@{adls_silver_gold_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_silver_root: {lakehouse_silver_root}")

lakehouse_gold_root = f"abfss://gold@{adls_silver_gold_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_gold_root: {lakehouse_gold_root}")

attunity_sap_ero_prelz_root = f"abfss://brewdat-ghq@{adls_brewdat_ghq_storage_account_name}.dfs.core.windows.net"
print(f"attunity_sap_ero_prelz_root: {attunity_sap_ero_prelz_root}")

