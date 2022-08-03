# Databricks notebook source
dbutils.widgets.text("brewdat_library_version", "v0.3.0", "1 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"brewdat_library_version: {brewdat_library_version}")

dbutils.widgets.text("source_system", "attunity_sap_ero", "2 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"source_system: {source_system}")

dbutils.widgets.text("target_zone", "ghq", "3 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"target_zone: {target_zone}")

dbutils.widgets.text("target_business_domain", "tech", "4 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"target_business_domain: {target_business_domain}")

dbutils.widgets.text("target_hive_database", "slv_ghq_tech_attunity_sap_ero", "5 - target_hive_database")
target_hive_database = dbutils.widgets.get("target_hive_database")
print(f"target_hive_database: {target_hive_database}")

dbutils.widgets.text("target_hive_table", "bkpf", "6 - target_hive_table")
target_hive_table = dbutils.widgets.get("target_hive_table")
print(f"target_hive_table: {target_hive_table}")

dbutils.widgets.text("source_hive_database", "brz_ghq_tech_attunity_sap_ero", "7 - source_hive_database")
source_hive_database = dbutils.widgets.get("source_hive_database")
print(f"source_hive_database: {source_hive_database}")

dbutils.widgets.text("data_interval_start", "2022-06-21T00:00:00Z", "8 - data_interval_start")
data_interval_start = dbutils.widgets.get("data_interval_start")
print(f"data_interval_start: {data_interval_start}")

dbutils.widgets.text("data_interval_end", '', "9 - data_interval_end")
data_interval_end = dbutils.widgets.get("data_interval_end")
print(f"data_interval_end: {data_interval_end}")

dbutils.widgets.text("silver_schema", "10 - silver_schema")
silver_schema = dbutils.widgets.get("silver_schema")
print(f"silver_schema: {silver_schema}")

dbutils.widgets.text("key_columns","MANDT,BUKRS,BELNR,GJAHR", "12 - key_columns")
key_columns = dbutils.widgets.get("key_columns")
print(f"key_columns: {key_columns}")

dbutils.widgets.text("watermark_column","target_apply_ts", "13 - watermark_column")
watermark_column = dbutils.widgets.get("watermark_column")
print(f"watermark_column: {watermark_column}")

# COMMAND ----------

silver_schema = """[{"source_table_name": "kna1", "source_attribute_name": "adrnr", "target_attribute_name": "adrnr",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "aufsd", "target_attribute_name": "aufsd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "begru", "target_attribute_name": "begru",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bran4", "target_attribute_name": "bran4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ccc01", "target_attribute_name": "ccc01",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ccc03", "target_attribute_name": "ccc03",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear1", "target_attribute_name": "dear1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "inspbydebi", "target_attribute_name": "inspbydebi",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "j_1kftind", "target_attribute_name": "j_1kftind",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr2", "target_attribute_name": "katr2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr4", "target_attribute_name": "katr4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kdkg4", "target_attribute_name": "kdkg4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "knazk", "target_attribute_name": "knazk",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "land1", "target_attribute_name": "land1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "locco", "target_attribute_name": "locco",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "lzone", "target_attribute_name": "lzone",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "name1", "target_attribute_name": "name1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ort02", "target_attribute_name": "ort02",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psofg", "target_attribute_name": "psofg",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psoo3", "target_attribute_name": "psoo3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psoo5", "target_attribute_name": "psoo5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psovn", "target_attribute_name": "psovn",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "regio", "target_attribute_name": "regio",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "rpmkr", "target_attribute_name": "rpmkr",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "sortl", "target_attribute_name": "sortl",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "spras", "target_attribute_name": "spras",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcd2", "target_attribute_name": "stcd2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcd4", "target_attribute_name": "stcd4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stceg", "target_attribute_name": "stceg",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stras", "target_attribute_name": "stras",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "telf1", "target_attribute_name": "telf1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "umjah", "target_attribute_name": "umjah",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_dpoint", "target_attribute_name": "vso_r_dpoint",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_load_pref", "target_attribute_name": "vso_r_load_pref",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_matpal", "target_attribute_name": "vso_r_matpal",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_one_sort", "target_attribute_name": "vso_r_one_sort",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_palhgt", "target_attribute_name": "vso_r_palhgt",
  "target_data_type": "double"},
 {"source_table_name": "kna1", "source_attribute_name": "xcpdk", "target_attribute_name": "xcpdk",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "xknza", "target_attribute_name": "xknza",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "xsubt", "target_attribute_name": "xsubt",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "xzemp", "target_attribute_name": "xzemp",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "alc", "target_attribute_name": "alc",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bahns", "target_attribute_name": "bahns",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bbsnr", "target_attribute_name": "bbsnr",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "cfopc", "target_attribute_name": "cfopc",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "civve", "target_attribute_name": "civve",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "datlt", "target_attribute_name": "datlt",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear6", "target_attribute_name": "dear6",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dtaws", "target_attribute_name": "dtaws",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "fityp", "target_attribute_name": "fityp",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "hzuor", "target_attribute_name": "hzuor",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "j_1kfrepre", "target_attribute_name": "j_1kfrepre",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr1", "target_attribute_name": "katr1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr10", "target_attribute_name": "katr10",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kdkg1", "target_attribute_name": "kdkg1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kdkg2", "target_attribute_name": "kdkg2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kdkg3", "target_attribute_name": "kdkg3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kunnr", "target_attribute_name": "kunnr",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "name2", "target_attribute_name": "name2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "name3", "target_attribute_name": "name3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "periv", "target_attribute_name": "periv",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pfach", "target_attribute_name": "pfach",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pfort", "target_attribute_name": "pfort",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psoo1", "target_attribute_name": "psoo1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psost", "target_attribute_name": "psost",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "sperz", "target_attribute_name": "sperz",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcdt", "target_attribute_name": "stcdt",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stkzu", "target_attribute_name": "stkzu",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "teltx", "target_attribute_name": "teltx",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "txjcd", "target_attribute_name": "txjcd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_i_no_lyr", "target_attribute_name": "vso_r_i_no_lyr",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_uld_side", "target_attribute_name": "vso_r_uld_side",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "werks", "target_attribute_name": "werks",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "xicms", "target_attribute_name": "xicms",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "abrvw", "target_attribute_name": "abrvw",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bbbnr", "target_attribute_name": "bbbnr",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "bran5", "target_attribute_name": "bran5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bubkz", "target_attribute_name": "bubkz",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "ccc02", "target_attribute_name": "ccc02",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ccc04", "target_attribute_name": "ccc04",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ernam", "target_attribute_name": "ernam",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "exabl", "target_attribute_name": "exabl",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "faksd", "target_attribute_name": "faksd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "gform", "target_attribute_name": "gform",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "inspatdebi", "target_attribute_name": "inspatdebi",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "j_1kftbus", "target_attribute_name": "j_1kftbus",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "jmjah", "target_attribute_name": "jmjah",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "katr3", "target_attribute_name": "katr3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr5", "target_attribute_name": "katr5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kdkg5", "target_attribute_name": "kdkg5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "knrza", "target_attribute_name": "knrza",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "konzs", "target_attribute_name": "konzs",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "mandt", "target_attribute_name": "mandt",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "mcod3", "target_attribute_name": "mcod3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "milve", "target_attribute_name": "milve",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "niels", "target_attribute_name": "niels",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psohs", "target_attribute_name": "psohs",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psois", "target_attribute_name": "psois",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pson1", "target_attribute_name": "pson1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psoo2", "target_attribute_name": "psoo2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcd1", "target_attribute_name": "stcd1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcd3", "target_attribute_name": "stcd3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_one_mat", "target_attribute_name": "vso_r_one_mat",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_pk_mat", "target_attribute_name": "vso_r_pk_mat",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "anred", "target_attribute_name": "anred",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bahne", "target_attribute_name": "bahne",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bran1", "target_attribute_name": "bran1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bran2", "target_attribute_name": "bran2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "bran3", "target_attribute_name": "bran3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "brsch", "target_attribute_name": "brsch",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "cassd", "target_attribute_name": "cassd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "cityc", "target_attribute_name": "cityc",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "confs", "target_attribute_name": "confs",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "counc", "target_attribute_name": "counc",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear2", "target_attribute_name": "dear2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear3", "target_attribute_name": "dear3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear4", "target_attribute_name": "dear4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dear5", "target_attribute_name": "dear5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "dtams", "target_attribute_name": "dtams",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "duefl", "target_attribute_name": "duefl",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ekont", "target_attribute_name": "ekont",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "erdat", "target_attribute_name": "erdat",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "etikg", "target_attribute_name": "etikg",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "fiskn", "target_attribute_name": "fiskn",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "jmzah", "target_attribute_name": "jmzah",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "katr6", "target_attribute_name": "katr6",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr7", "target_attribute_name": "katr7",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr8", "target_attribute_name": "katr8",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "katr9", "target_attribute_name": "katr9",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "knurl", "target_attribute_name": "knurl",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ktocd", "target_attribute_name": "ktocd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ktokd", "target_attribute_name": "ktokd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "kukla", "target_attribute_name": "kukla",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "lifnr", "target_attribute_name": "lifnr",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "lifsd", "target_attribute_name": "lifsd",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "loevm", "target_attribute_name": "loevm",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "mcod1", "target_attribute_name": "mcod1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "mcod2", "target_attribute_name": "mcod2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "name4", "target_attribute_name": "name4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "nodel", "target_attribute_name": "nodel",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "ort01", "target_attribute_name": "ort01",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pmt_office", "target_attribute_name": "pmt_office",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pson2", "target_attribute_name": "pson2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pson3", "target_attribute_name": "pson3",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psoo4", "target_attribute_name": "psoo4",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "psotl", "target_attribute_name": "psotl",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pstl2", "target_attribute_name": "pstl2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "pstlz", "target_attribute_name": "pstlz",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "sperr", "target_attribute_name": "sperr",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stcd5", "target_attribute_name": "stcd5",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stkza", "target_attribute_name": "stkza",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "stkzn", "target_attribute_name": "stkzn",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "telbx", "target_attribute_name": "telbx",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "telf2", "target_attribute_name": "telf2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "telfx", "target_attribute_name": "telfx",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "telx1", "target_attribute_name": "telx1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "txlw1", "target_attribute_name": "txlw1",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "txlw2", "target_attribute_name": "txlw2",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "umsa1", "target_attribute_name": "umsa1",
  "target_data_type": "double"},
 {"source_table_name": "kna1", "source_attribute_name": "umsat", "target_attribute_name": "umsat",
  "target_data_type": "double"},
 {"source_table_name": "kna1", "source_attribute_name": "updat", "target_attribute_name": "updat",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "uptim", "target_attribute_name": "uptim",
  "target_data_type": "long"},
 {"source_table_name": "kna1", "source_attribute_name": "uwaer", "target_attribute_name": "uwaer",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vbund", "target_attribute_name": "vbund",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "vso_r_pal_ul", "target_attribute_name": "vso_r_pal_ul",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "xxipi", "target_attribute_name": "xxipi",
  "target_data_type": "string"},
 {"source_table_name": "kna1", "source_attribute_name": "SOURCE_COMMIT_TS",
  "target_attribute_name": "SOURCE_COMMIT_TS", "target_data_type": "timestamp"},
 {"source_table_name": "kna1", "source_attribute_name": "TARGET_APPLY_TS",
  "target_attribute_name": "TARGET_APPLY_TS", "target_data_type": "timestamp"},
 {"source_table_name": "kna1", "source_attribute_name": "SOURCE_COMMIT_DT",
  "target_attribute_name": "SOURCE_COMMIT_DT", "target_data_type": "date"},
 {"source_table_name": "kna1", "source_attribute_name": "TARGET_APPLY_DT",
  "target_attribute_name": "TARGET_APPLY_DT", "target_data_type": "date"},
 {"source_table_name": "kna1", "source_attribute_name": "op_ind", "target_attribute_name": "op_ind",
  "target_data_type": "string"}
 ]"""

# COMMAND ----------

import json
import sys
import pyspark.sql.functions as F
from ast import literal_eval
import datetime

silver_schema = literal_eval(silver_schema)
key_columns_list = key_columns.split(",")

# COMMAND ----------

import re
from datetime import datetime
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from pyspark.sql.window import Window

class RowSchema():
    def __init__(
            self,
            column_map: dict
    ):
        self.source_attribute_name = column_map["source_attribute_name"]
        self.target_attribute_name = column_map["target_attribute_name"]
        self.target_data_type = column_map["target_data_type"]
        
schema = []
for item in silver_schema:
    schema.append(RowSchema(item))
    
def apply_schema(
    dbutils: object,
    df: DataFrame,
    schema: List[str]
) -> DataFrame:
    target_columns = [col["source_attribute_name"] for col in ]
    for 
    expressions = []
    for s in schema:    

# COMMAND ----------

schema = brz_df.schema
print(schema.fields)

# COMMAND ----------

def apply_silver_schema(
    dbutils: object,
    df: DataFrame,
    silver_schema: list
) -> DataFrame:
    """Cast all DataFrame columns to required data types and change column names 
    as received from the input schema.

    Parameters
    ----------
    dbutils : object
        A Databricks utils object.
    df : DataFrame
        The PySpark DataFrame to cast.
    silver_schema: List
        List containing column details of silver table. The element of the list is a dictionary containing
        source_table_name, source_column_name, target_data_type, target_column_name

    Returns
    -------
    DataFrame
        The modified PySpark DataFrame with all columns cast to required data types as specified in schema.
    """
    try:
        expressions = []
        for column in df.schema:
            target_data_type, target_attribute_name = _get_target_data_type(column.name, silver_schema)
            if target_data_type == 'string':
                expressions.append(f"`{column.name}` AS `{target_attribute_name}`")
            else:
                expressions.append(f"CAST(`{column.name}` AS {target_data_type}) AS `{target_attribute_name}`")
        return df.selectExpr(*expressions)       
    except Exception:
        common_utils.exit_with_last_exception(dbutils)
        
def _get_target_data_type(column_name, silver_schema) -> str:
    """Finds the data type of input column in input schema. 

    Parameters
    ----------
    column_name : string
        Input Column Name .
    silver_schema: List
        List containing column details of silver table. The element of the list is a dictionary containing
        source_table_name, source_column_name, target_data_type, target_column_name

    Returns
    -------
    str
        Target data type the input column needs to be cast to
    """
    for item in silver_schema:
        if item["source_attribute_name"].lower() == column_name.lower():
            return item["target_data_type"], item["target_attribute_name"]
    return "string", column_name

# COMMAND ----------

# Import BrewDat Library modules
#sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
sys.path.append(f"/Workspace/Repos/sachin.kumar@ab-inbev.com/brewdat-pltfrm-ghq-tech-template-adb")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils

# Print a module's help
help(read_utils)

# COMMAND ----------

x = common_utils.ReturnObject
print(x.)

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    spark=spark,
    dbutils=dbutils,
    storage_account_names=[
        adls_raw_bronze_storage_account_name,
        adls_silver_gold_storage_account_name,
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

convert_watermark_format = lambda x : datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

brz_df = spark.sql(f"select * from {source_hive_database}.{target_hive_table} where TARGET_APPLY_DT >= TO_DATE('{data_interval_start}')")
if not data_interval_end:
    watermark_upper_bound = brz_df.select(F.max(F.col(watermark_column))).collect()[0][0]
    data_interval_end = convert_watermark_format(watermark_upper_bound)

print(data_interval_start, data_interval_end)
filtered_df = brz_df.filter(F.col(watermark_column).between(
        F.to_timestamp(F.lit(data_interval_start)),
        F.to_timestamp(F.lit(data_interval_end)),
    ))

df = transform_utils.apply_silver_schema(dbutils, filtered_df, silver_schema)

# COMMAND ----------

dedup_df = transform_utils.deduplicate_records(
    dbutils=dbutils,
    df=df,
    key_columns=key_columns_list,
    watermark_column=watermark_column,
)

#display(dedup_df)

# COMMAND ----------

audit_df = transform_utils.create_or_replace_audit_columns(dbutils=dbutils, df=dedup_df)

# COMMAND ----------

location = lakehouse_utils.generate_silver_table_location(
    dbutils=dbutils,
    lakehouse_silver_root=lakehouse_silver_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_hive_table,
)
print(location)

results = write_utils.write_delta_table(
    spark=spark,
    df=audit_df,
    location=location,
    schema_name=target_hive_database,
    table_name=target_hive_table,
    load_type=write_utils.LoadType.UPSERT,
    key_columns=key_columns_list,
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    update_condition="source.source_commit_ts > target.source_commit_ts"
)
vars(results)["data_interval_end"] = data_interval_end
print(vars(results))

# COMMAND ----------

common_utils.exit_with_object(dbutils=dbutils, results=results)
