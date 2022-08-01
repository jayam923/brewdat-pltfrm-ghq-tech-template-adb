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

# COMMAND ----------

silver_schema1 = [
    {"source_table_name": "kna1", "source_attribute_name": "abrvw", "target_attribute_name": "usage_indicator",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "adrnr", "target_attribute_name": "master_address",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "anred", "target_attribute_name": "title",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bahne", "target_attribute_name": "express_train_station",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bahns", "target_attribute_name": "train_station",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bran4", "target_attribute_name": "industry_code_4",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "cassd", "target_attribute_name": "central_salesblock",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "cfopc", "target_attribute_name": "customers_cfop_category",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "cityc", "target_attribute_name": "master_city_code",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dtams", "target_attribute_name": "indicator_for_data_medium_exchange",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dtaws","target_attribute_name": "instruction_key_for_data_medium_exchange", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ernam", "target_attribute_name": "record_created_by",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "exabl",
                                     "target_attribute_name": "indicator_unloading_points_exist",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "inspatdebi",
     "target_attribute_name": "inspection_for_a_delivery_note_after_outbound_delivery", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "j_1kfrepre",
     "target_attribute_name": "name_of_representative", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr2", "target_attribute_name": "customer_attribute_2",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr4", "target_attribute_name": "customer_attribute_4",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "kdkg4",
                                     "target_attribute_name": "customer_condition_group_4",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "knazk", "target_attribute_name": "working_time_calendar",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "knrza",
                                     "target_attribute_name": "account_number_of_an_alternative_payer",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "knurl", "target_attribute_name": "uniform_resource_locator",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "ktocd",
                                     "target_attribute_name": "reference_account_group_for_one_time_account_customer",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "kunnr", "target_attribute_name": "customer_id",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "land1", "target_attribute_name": "country_id",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "lifnr", "target_attribute_name": "master_vendor",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ort01", "target_attribute_name": "customer_location",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psofg", "target_attribute_name": "processor_group",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pstl2", "target_attribute_name": "po_box_postal_code",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "rpmkr", "target_attribute_name": "regional_market",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcd3", "target_attribute_name": "master_tax_number_3",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcdt", "target_attribute_name": "tax_number_type",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stkzn", "target_attribute_name": "natural_person",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "telbx", "target_attribute_name": "telebox_number",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "telf1", "target_attribute_name": "master_telephone",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "telf2", "target_attribute_name": "second_telephone_number",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "txjcd", "target_attribute_name": "tax_jurisdiction",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "umsa1", "target_attribute_name": "annual_sales",
     "target_data_type": "double"},
    {"source_table_name": "kna1", "source_attribute_name": "updat", "target_attribute_name": "date_on_changes_confirm",
     "target_data_type": "long"}, {"source_table_name": "kna1", "source_attribute_name": "vbund",
                                   "target_attribute_name": "trading_partner_company_id", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_i_no_lyr",
     "target_attribute_name": "number_layers_under_an_inter_pallet_vso", "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "xcpdk",
     "target_attribute_name": "master_onetime_account_indicator", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "xicms", "target_attribute_name": "customer_is_icms_exempt",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "xknza",
                                     "target_attribute_name": "indicator_alternative_payer_using_account_number",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "xxipi", "target_attribute_name": "customer_is_ipi_exempt",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "aufsd", "target_attribute_name": "master_orderblock",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "begru", "target_attribute_name": "authorization_group",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bran3", "target_attribute_name": "industry_code_3",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "ccc01",
                                     "target_attribute_name": "indicator_for_biochemical_warfare_for_legal_control",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ccc02",
     "target_attribute_name": "indicator_for_nuclear_nonproliferation_for_legal_control", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ccc04",
     "target_attribute_name": "indicator_for_missile_technology_for_legal_control", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "civve",
     "target_attribute_name": "id_for_mainly_non_military_use", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "counc", "target_attribute_name": "county_code",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dear1", "target_attribute_name": "indicator_competitor",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dear2", "target_attribute_name": "indicator_sales_partner",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dear3", "target_attribute_name": "indicator_sales_prospect",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "duefl",
                                     "target_attribute_name": "status_of_data_transfer_into_subsequent_release",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "etikg",
     "target_attribute_name": "is_r_labeling_customer_plant_group", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "fityp", "target_attribute_name": "tax_type",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "hzuor", "target_attribute_name": "assignment_to_hierarchy",
     "target_data_type": "long"}, {"source_table_name": "kna1", "source_attribute_name": "jmjah",
                                   "target_attribute_name": "year_for_which_the_number_of_employees_is_given",
                                   "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "jmzah",
     "target_attribute_name": "yearly_number_of_employees", "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "katr1", "target_attribute_name": "customer_attribute_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr7", "target_attribute_name": "customer_attribute_7",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr9", "target_attribute_name": "customer_attribute_9",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "kdkg1",
                                     "target_attribute_name": "customer_condition_group_1",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "kdkg3",
     "target_attribute_name": "customer_condition_group_3", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "lifsd", "target_attribute_name": "master_delivery_block",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "loevm", "target_attribute_name": "master_deletion_flag",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "milve",
                                     "target_attribute_name": "id_for_mainly_military_use",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "name2", "target_attribute_name": "customer_name_2",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pfach", "target_attribute_name": "po_box",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pfort", "target_attribute_name": "po_box_city",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "psois",
                                     "target_attribute_name": "subledger_acct_preprocessing_procedure",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pson1", "target_attribute_name": "name_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pson2", "target_attribute_name": "name_2",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pson3", "target_attribute_name": "name_3",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psoo5", "target_attribute_name": "description_4",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "regio", "target_attribute_name": "customer_region",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "sortl", "target_attribute_name": "customer_short_search",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "sperr", "target_attribute_name": "central_posting_block",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcd1", "target_attribute_name": "tax_number_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcd5", "target_attribute_name": "steuernummer_5",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stras", "target_attribute_name": "location_address",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "telfx", "target_attribute_name": "master_fax_number",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "umjah",
                                     "target_attribute_name": "year_for_which_sales_are_given",
                                     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "umsat", "target_attribute_name": "annual_sales_1",
     "target_data_type": "double"}, {"source_table_name": "kna1", "source_attribute_name": "vso_r_dpoint",
                                     "target_attribute_name": "collective_unloading_point_for_vso",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_palhgt",
     "target_attribute_name": "maximum_stacking_height_of_the_packaging_material_vso", "target_data_type": "double"},
    {"source_table_name": "kna1", "source_attribute_name": "werks", "target_attribute_name": "plant",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "xzemp",
                                     "target_attribute_name": "indicator_is_an_alternative_payer_allowed_in_document",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "alc", "target_attribute_name": "agency_location_code",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "bbsnr",
                                     "target_attribute_name": "international_location_number_part_2",
                                     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "bran1", "target_attribute_name": "industry_code_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bran5", "target_attribute_name": "industry_code_5",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "brsch", "target_attribute_name": "industry_key",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "dear4",
                                     "target_attribute_name": "indicator_for_customer_type_4",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dear5",
     "target_attribute_name": "id_for_default_sold_to_party", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "erdat", "target_attribute_name": "record_created_date",
     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "faksd", "target_attribute_name": "master_billing_block",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "fiskn",
                                     "target_attribute_name": "account_number_of_the_master_record_with_the_fiscal_address",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "inspbydebi",
     "target_attribute_name": "inspection_carried_out_by_customer_no_inspection_lot", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "j_1kftbus", "target_attribute_name": "type_of_business",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr3", "target_attribute_name": "customer_attribute_3",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr5", "target_attribute_name": "customer_attribute_5",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "kdkg5",
                                     "target_attribute_name": "customer_condition_group_5",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "kukla", "target_attribute_name": "master_customer_class",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "locco", "target_attribute_name": "city_coordinates",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "mcod3",
                                     "target_attribute_name": "search_term_for_matchcode_search",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "name1", "target_attribute_name": "customer_name_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "niels", "target_attribute_name": "nielsen_id",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ort02", "target_attribute_name": "district",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pmt_office", "target_attribute_name": "payment_office",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "psohs",
                                     "target_attribute_name": "house_number_is_no_longer_used_from_release",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psoo4", "target_attribute_name": "description_3",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psovn", "target_attribute_name": "first_name",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "spras", "target_attribute_name": "master_language",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcd4", "target_attribute_name": "master_tax_number_4",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stceg", "target_attribute_name": "tax_reg_number",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "stkza",
                                     "target_attribute_name": "indicator_business_partner_subject_to_equalization_tax",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stkzu", "target_attribute_name": "liable_for_vat",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "teltx", "target_attribute_name": "teletex_number",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "telx1", "target_attribute_name": "telex_number",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "txlw2", "target_attribute_name": "tax_law_ipi",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "uptim", "target_attribute_name": "time_on_changes_confirm",
     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "uwaer", "target_attribute_name": "currency_of_sales_figure",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "vso_r_load_pref",
                                     "target_attribute_name": "front_back_preference_of_loading_unloading_vso",
                                     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_pk_mat",
     "target_attribute_name": "customer_related_packing_for_each_packaging_material_vso", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "xsubt",
     "target_attribute_name": "customer_group_for_calculation", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "bbbnr",
     "target_attribute_name": "international_location_number_part_1", "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "bran2", "target_attribute_name": "industry_code_2",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "bubkz",
                                     "target_attribute_name": "check_digit_for_the_international_location_number",
                                     "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "ccc03",
     "target_attribute_name": "indicator_for_national_security_for_legal_control", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "confs",
     "target_attribute_name": "status_of_change_authorization_central", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "datlt",
     "target_attribute_name": "data_communication_line_no", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "dear6", "target_attribute_name": "consumer_ind",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ekont", "target_attribute_name": "initial_contact",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "gform", "target_attribute_name": "legal_status",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "j_1kftind", "target_attribute_name": "type_of_industry",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr10", "target_attribute_name": "customer_attribute_10",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr6", "target_attribute_name": "customer_attribute_6",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "katr8", "target_attribute_name": "customer_attribute_8",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "kdkg2",
                                     "target_attribute_name": "customer_condition_group_2",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "konzs", "target_attribute_name": "master_group",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "ktokd", "target_attribute_name": "master_account_group",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "lzone", "target_attribute_name": "transportation_zone",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "mandt", "target_attribute_name": "system_id",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "mcod1",
     "target_attribute_name": "search_term_for_matchcode_search_1", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "mcod2",
     "target_attribute_name": "search_term_for_matchcode_search_2", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "name3", "target_attribute_name": "name_6",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "name4", "target_attribute_name": "name_4",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "nodel", "target_attribute_name": "master_deletion_block",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "periv", "target_attribute_name": "fiscal_year_variant",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psoo1", "target_attribute_name": "description",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psoo2", "target_attribute_name": "description_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psoo3", "target_attribute_name": "description_2",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "psost",
                                     "target_attribute_name": "street_no_longer_used_from_release",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "psotl", "target_attribute_name": "title_1",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "pstlz", "target_attribute_name": "master_postal_code",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "sperz", "target_attribute_name": "payment_block",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "stcd2", "target_attribute_name": "master_tax_number_2",
     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "txlw1", "target_attribute_name": "tax_law_icms",
     "target_data_type": "string"}, {"source_table_name": "kna1", "source_attribute_name": "vso_r_matpal",
                                     "target_attribute_name": "packaging_material_of_the_customer_in_vso",
                                     "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_one_mat",
     "target_attribute_name": "packing_material_specific_for_each_packaging_material", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_one_sort",
     "target_attribute_name": "pack_only_one_package_type_for_each_pkm_vso", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_pal_ul",
     "target_attribute_name": "unit_of_length_for_packaging_material_vso", "target_data_type": "string"},
    {"source_table_name": "kna1", "source_attribute_name": "vso_r_uld_side",
     "target_attribute_name": "side_preference_of_loading_unloading_vso", "target_data_type": "long"},
    {"source_table_name": "kna1", "source_attribute_name": "SOURCE_COMMIT_TS",
     "target_attribute_name": "SOURCE_COMMIT_TS", "target_data_type": "timestamp"},
    {"source_table_name": "kna1", "source_attribute_name": "TARGET_APPLY_TS",
     "target_attribute_name": "TARGET_APPLY_TS", "target_data_type": "timestamp"},
    {"source_table_name": "kna1", "source_attribute_name": "SOURCE_COMMIT_DT",
     "target_attribute_name": "SOURCE_COMMIT_DT", "target_data_type": "date"},
    {"source_table_name": "kna1", "source_attribute_name": "TARGET_APPLY_DT",
     "target_attribute_name": "TARGET_APPLY_DT", "target_data_type": "date"},
    {"source_table_name": "kna1", "source_attribute_name": "op_ind", "target_attribute_name": "op_ind",
     "target_data_type": "string"}]

