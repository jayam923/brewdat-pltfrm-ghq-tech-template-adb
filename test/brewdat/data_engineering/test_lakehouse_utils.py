import pytest

from brewdat.data_engineering.lakehouse_utils import assert_valid_folder_name, generate_bronze_table_location, generate_silver_table_location, generate_gold_table_location


def test_check_table_name_valid_names():
    assert_valid_folder_name("new_table")
    assert_valid_folder_name("new_table123")
    assert_valid_folder_name("123_new_table123")
    assert_valid_folder_name("new-table123")
    assert_valid_folder_name("new.table123")


def test_check_table_name_invalid_name1():
    with pytest.raises(ValueError):
        assert_valid_folder_name("_new_table")


def test_check_table_name_invalid_name2():
    with pytest.raises(ValueError):
        assert_valid_folder_name("new+table")


def test_check_table_name_invalid_name3():
    with pytest.raises(ValueError):
        assert_valid_folder_name("new_table_ç")


def test_generate_bronze_table_location():
    # ACT
    result = generate_bronze_table_location(dbutils=None,
                                            lakehouse_bronze_root="abfss://bronze@storage_account.dfs.core.windows.net",
                                            target_zone="ghq",
                                            target_business_domain="tech",
                                            source_system="sap_tech",
                                            table_name="extraction_table"
                                            )
    
    # ASSERT
    assert result == "abfss://bronze@storage_account.dfs.core.windows.net/data/ghq/tech/sap_tech/extraction_table"


def test_generate_silver_table_location():
    # ACT
    result = generate_silver_table_location(dbutils=None,
                                            lakehouse_silver_root="abfss://silver@storage_account.dfs.core.windows.net",
                                            target_zone="ghq",
                                            target_business_domain="tech",
                                            source_system="sap_tech",
                                            table_name="extraction_table"
                                            )
    
    # ASSERT
    assert result == "abfss://silver@storage_account.dfs.core.windows.net/data/ghq/tech/sap_tech/extraction_table"


def test_generate_gold_table_location():
    # ACT
    result = generate_gold_table_location(dbutils=None,
                                            lakehouse_gold_root="abfss://gold@storage_account.dfs.core.windows.net",
                                            target_zone="ghq",
                                            target_business_domain="tech",
                                            data_product="Plataform",
                                            database_name="Gold_database",
                                            table_name="extraction_table"
                                            )
    
    # ASSERT
    assert result == "abfss://gold@storage_account.dfs.core.windows.net/data/ghq/tech/plataform/gold_database/extraction_table"
