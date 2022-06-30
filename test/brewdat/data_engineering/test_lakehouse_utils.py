import pytest

from brewdat.data_engineering.lakehouse_utils import check_table_name


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
