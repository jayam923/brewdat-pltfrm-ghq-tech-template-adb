# Databricks notebook source
from test_write_utils import *

# COMMAND ----------

from datetime import datetime
current_ts = datetime.strftime(datetime.utcnow(),'%Y%m%d%H%M%S')

# COMMAND ----------

tmpdir = f"/dbfs/tmp/test_write/{current_ts}"
dbutils.fs.rm("dbfs:/tmp/test_write/", recurse=True)
spark.sql("DROP DATABASE IF EXISTS test_schema CASCADE")
dbutils.fs.mkdirs(tmpdir)

# COMMAND ----------

test_write_delta_table_append_all(tmpdir)
test_location_already_exists(tmpdir)
test_write_scd_type_2_first_write(tmpdir)
test_write_scd_type_2_only_new_ids(tmpdir)
test_write_scd_type_2_only_updates(tmpdir)
test_write_scd_type_2_same_id_same_data(tmpdir)
test_write_scd_type_2_updates_and_new_records(tmpdir)
test_write_scd_type_2_multiple_keys(tmpdir)
test_write_scd_type_2_schema_evolution(tmpdir)
test_write_scd_type_2_partition(tmpdir)
test_write_scd_type_2_struct_types(tmpdir)
test_write_duplicated_data_for_upsert(tmpdir)
test_write_delta_table_append_new(tmpdir)
test_write_delta_table_overwrite_table(tmpdir)
test_write_delta_table_overwrite_partition(tmpdir)
test_write_delta_table_upsert(tmpdir)
test_append_upsert_load_count(tmpdir)
test_append_upsert_with_nulls_load_count(tmpdir)
test_append_new_load_count(tmpdir)
test_type2_scd_load_count(tmpdir)
test_write_bad_records_write_to_error_location_mode(tmpdir)
test_write_bad_records_ignore_mode(tmpdir)
