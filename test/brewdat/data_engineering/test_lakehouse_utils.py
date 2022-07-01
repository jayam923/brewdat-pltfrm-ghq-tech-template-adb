import pytest

from brewdat.data_engineering.lakehouse_utils import check_table_name, generate_bronze_table_location


def test_check_table_name_valid_names():
    check_table_name("new_table")
    check_table_name("new_table123")
    check_table_name("123_new_table123")
    check_table_name("new-table123")
    check_table_name("new.table123")


def test_check_table_name_invalid_name1():
    with pytest.raises(ValueError):
        check_table_name("_new_table")


def test_check_table_name_invalid_name2():
    with pytest.raises(ValueError):
        check_table_name("new+table")


def test_check_table_name_invalid_name3():
    with pytest.raises(ValueError):
        check_table_name("new_table_ç")
 


def test_generate_bronze_table_location():
    # ARRANGE
    
    # ACT
    result = generate_bronze_table_location(dbutils=None,lakehouse_bronze_root= "data"/{"target_zone"}/{"target_business_domain"}/{"source_system"}/{"table_name"})
    
    # ASSERT
    assert "lakehouse_bronze_root" in result.columns
