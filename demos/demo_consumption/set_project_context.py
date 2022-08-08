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
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldd"
    blob_storage_account_name = "brewdatpltfrmsynwkssad"
    key_vault_name = "brewdatpltfrmghqtechakvd"
    spn_client_id = "1d3aebfe-929c-4cc1-a988-31c040d2b798"
    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-d"
    url_syn_jdbc   = "jdbc:sqlserver://brewdat-pltfrm-synwks-d.sql.azuresynapse.net:1433;database=poc_sqlpool;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated"
elif environment == "qa":
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldq"
    key_vault_name = "brewdatpltfrmghqtechakvq"
    spn_client_id = "12345678-1234-1234-1234-123456789999"
    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-q"
elif environment == "prod":
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldp"
    key_vault_name = "brewdatpltfrmghqtechakvp"
    spn_client_id = "12345678-1234-1234-1234-123456789999"
    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-p"

print(f"adls_silver_gold_storage_account_name: {adls_silver_gold_storage_account_name}")
print(f"key_vault_name: {key_vault_name}")
print(f"spn_client_id: {spn_client_id}")
print(f"spn_secret_name: {spn_secret_name}")

# COMMAND ----------

# Export additional helper variables
lakehouse_silver_root = f"abfss://silver@{adls_silver_gold_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_silver_root: {lakehouse_silver_root}")

lakehouse_gold_root = f"abfss://gold@{adls_silver_gold_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_gold_root: {lakehouse_gold_root}")

lakehouse_blob_root = f"abfss://temp-csa@{blob_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_blob_root: {lakehouse_blob_root}") 
