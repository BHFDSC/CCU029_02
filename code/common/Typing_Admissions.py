# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../config/quiet $skip=skip

# COMMAND ----------

unpack_config(config)

# COMMAND ----------

# MAGIC %run ./utils/TABLE_NAMES

# COMMAND ----------

# MAGIC %run ./utils/admission_type_functions

# COMMAND ----------

try:
    test
except:
    test = True

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

input_table_name = f"{preamble}_{admissions_w_icu_output_table_name}"
output_table_name = f"{preamble}_{admissions_typed_output_table_name}"

print("Applying the hierachy of type definitions to all admissions...")

# COMMAND ----------

if "only_important_admissions" in config:
    query_string = create_typed_admissions_query(
        f"{collab_database}.{input_table_name}", important_only=config["only_important_admissions"]
    )
elif "old_pims" in config:
    query_string = create_typed_admissions_query(f"{collab_database}.{input_table_name}", old_pims=config["old_pims"])
else:
    query_string = create_typed_admissions_query(f"{collab_database}.{input_table_name}")

all_typed_admissions = spark.sql(query_string)

print(f"Creating `{output_table_name}` with study start date == {study_start}")

all_typed_admissions.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
