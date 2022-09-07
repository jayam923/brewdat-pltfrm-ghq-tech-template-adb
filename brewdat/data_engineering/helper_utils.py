from .import common_utils

def data_quality_narrow_check(column_level_mapping: object, dq_checker: object, dbutils: object):
    """Helper class that provides data quality narrow checks for given DataFrame.

    Parameters
    ----------
    column_level_mapping : Object
        DataQuality column mapping object.
    dq_checker : Object
        DataQuality uitls object
    dbutils : object
        A Databricks utils object.
    """
    try:
        mappings = [common_utils.DataQualityColumnMapping(**mapping) for mapping in column_level_mapping]
        # Apply data quality checks based on given column mappings
        for mapping in mappings:
            if mapping.check_max_length is not None:
                dq_checker = dq_checker.check_column_max_length(
                    mapping.source_column_name,
                    maximum_length=mapping.check_max_length
                )
            if mapping.check_min_length is not None:
                dq_checker = dq_checker.check_column_min_length(
                    mapping.source_column_name,
                    minimum_length=mapping.check_min_length
                )
            if mapping.check_max_value is not None:
                dq_checker = dq_checker.check_column_max_value(
                    mapping.source_column_name,
                    maximum_value=mapping.check_max_value
                )
            if mapping.check_min_value is not None:
                dq_checker = dq_checker.check_column_min_value(
                    mapping.source_column_name,
                    minimum_value=mapping.check_min_value
                )
            if mapping.check_valid_values:
                dq_checker = dq_checker.check_column_value_is_in(
                    mapping.source_column_name,
                    valid_values=mapping.check_valid_values
                )
            if mapping.check_invalid_values:
                dq_checker = dq_checker.check_column_value_is_not_in(
                    mapping.source_column_name,
                    invalid_values=mapping.check_invalid_values
                )
            if mapping.check_matches_regex is not None:
                dq_checker = dq_checker.check_column_matches_regular_expression(
                    mapping.source_column_name,
                    regular_expression=mapping.check_matches_regex
                )
            if mapping.check_not_matches_regex is not None:
                dq_checker = dq_checker.check_column_does_not_match_regular_expression(
                    mapping.source_column_name,
                    regular_expression=mapping.check_not_matches_regex
                )

    except Exception:
        common_utils.exit_with_last_exception(dbutils=dbutils)
        
def data_quality_wider_check(table_level_mapping: object, column_level_mapping: object, dq_checker: object, previous_version: int,current_version: int, dbutils: object):
    """Helper class that provides data quality wide checks for given DataFrame.

    Parameters
    ----------
    column_level_mapping : Object
        DataQuality column mapping object.
    column_level_mapping : Object
        DataQuality column mapping object.
    dq_checker : Object
        DataQuality uitls object.
    dbutils : object
        A Databricks utils object.
    """
    try:
        column_mapping = [common_utils.DataQualityColumnMapping(**mapping) for mapping in column_level_mapping]
        table_mapping = [common_utils.DataQualityColumnMapping(**mapping) for mapping in table_level_mapping]
        # Apply data quality checks based on given column mappings
        for mapping in column_mapping:
            if mapping.check_columns_null_variation:
                dq_checker.check_column_nulls( 
                    col_name = mapping.source_column_name,
                    mostly =mapping.check_columns_null_variation
                )

            if mapping.check_column_sum_values:
                dq_checker.check_column_sum( 
                    col_name = mapping.source_column_name,
                    min_value = mapping.check_column_sum_values[0],
                    max_value = mapping.check_column_sum_values[1]
                )

            if mapping.check_column_uniqueness_variation:
                dq_checker.check_column_uniqueness(
                col_name = mapping.source_column_name,
                    mostly = mapping.check_column_uniqueness_variation
                )

            if mapping.check_null_percentage_variation_from_previous_version:
                dq_checker.check_null_percentage_variation_from_previous_version(
                    col_name = mapping.source_column_name,
                    max_accepted_variation = mapping.check_null_percentage_variation_from_previous_version,
                    previous_version=previous_version,
                    current_version=current_version
                )

            if mapping.check_numeric_sum_varation_from_previous_version:
                dq_checker.check_numeric_sum_varation_from_previous_version(
                    col_name = mapping.source_column_name,
                    min_value = mapping.check_numeric_sum_varation_from_previous_version[0],
                    max_value = mapping.check_numeric_sum_varation_from_previous_version[1],
                    previous_version = previous_version,
                    current_version = current_version
                )

        for mapping in table_mapping:
            if mapping.check_compound_column_uniqueness_variation:
                dq_checker.check_compound_column_uniqueness(
                    col_list = mapping.compound_columns,
                    mostly = mapping.check_compound_column_uniqueness_variation
                )
            if mapping.check_row_count:
                dq_checker.check_row_count(
                min_value = mapping.check_row_count[0],
                max_value = mapping.check_row_count[1]
                )

            if mapping.check_count_variation_from_previous_version:
                dq_checker.check_count_variation_from_previous_version( 
                    min_value = mapping.check_count_variation_from_previous_version[0],
                    max_value = mapping.check_count_variation_from_previous_version[1],
                    previous_version = previous_version,
                    current_version = current_version)

            if mapping.check_bad_records_percentage:
                dq_checker.check_bad_records_percentage( 
                    min_percentage = mapping.check_bad_records_percentage[0],
                    max_percentage = mapping.check_bad_records_percentage[1],
                    current_version = current_version
                )

    except Exception:
        common_utils.exit_with_last_exception(dbutils=dbutils)