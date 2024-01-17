# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../02/config/quiet $skip=skip

# COMMAND ----------

unpack_config(config)

# COMMAND ----------

# MAGIC %run ./utils/TABLE_NAMES

# COMMAND ----------

# MAGIC %run ./utils/UHC_functions

# COMMAND ----------

try:
  test
except:
  test = True

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

input_table_name = f"{preamble}_{cohort_w_zscores_output_table_name}"
cohort_w_no_gdppr_md_uhcs_table_name = f"{preamble}_{cohort_w_no_gdppr_md_uhcs_table_name}"
cohort_w_no_gdppr_green_book_uhcs_table_name = f"{preamble}_{cohort_w_no_gdppr_green_book_uhcs_table_name}"
cohort_w_md_uhcs_table_name = f"{preamble}_{cohort_w_md_uhcs_table_name}"
output_table_name = f"{preamble}_{cohort_w_all_uhcs_output_table_name}"

print("Reading in Kate's UHC definitions and generating query to identify them amongst the cohort...")

# COMMAND ----------

# Adding Underlying Health Conditions (UHCs) per Kate's spreadsheets.

# COMMAND ----------

no_gdppr_in = False
if "no_gdppr_uhcs" in config and config["no_gdppr_uhcs"] == True:
    cohort_with_no_gdppr_md_uhcs = formulate_uhcs("MD", f"{collab_database}.{input_table_name}", no_gdppr=True)
    print(f"Creating `{cohort_w_no_gdppr_md_uhcs_table_name}` with study start date == {study_start}")

    cohort_with_no_gdppr_md_uhcs.createOrReplaceTempView(cohort_w_no_gdppr_md_uhcs_table_name)
    drop_table(cohort_w_no_gdppr_md_uhcs_table_name)
    create_table(cohort_w_no_gdppr_md_uhcs_table_name)
    optimise_table(cohort_w_no_gdppr_md_uhcs_table_name, "PERSON_ID_DEID")

    table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_w_no_gdppr_md_uhcs_table_name}")
    print(f"`{cohort_w_no_gdppr_md_uhcs_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

    cohort_with_no_gdppr_green_book_uhcs = formulate_uhcs("GREEN_BOOK", f"{collab_database}.{cohort_w_no_gdppr_md_uhcs_table_name}", no_gdppr=True)
    print(f"Creating `{cohort_w_no_gdppr_green_book_uhcs_table_name}` with study start date == {study_start}")

    cohort_with_no_gdppr_green_book_uhcs.createOrReplaceTempView(cohort_w_no_gdppr_green_book_uhcs_table_name)
    drop_table(cohort_w_no_gdppr_green_book_uhcs_table_name)
    create_table(cohort_w_no_gdppr_green_book_uhcs_table_name)

    optimise_table(cohort_w_no_gdppr_green_book_uhcs_table_name, "PERSON_ID_DEID")

    table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_w_no_gdppr_green_book_uhcs_table_name}")
    print(f"`{cohort_w_no_gdppr_green_book_uhcs_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
    no_gdppr_in = True

# COMMAND ----------

cohort_with_md_uhcs = formulate_uhcs("MD", f"{collab_database}.{input_table_name if not no_gdppr_in else cohort_w_no_gdppr_green_book_uhcs_table_name}")
print(f"Creating `{cohort_w_md_uhcs_table_name}` with study start date == {study_start}")

cohort_with_md_uhcs.createOrReplaceTempView(cohort_w_md_uhcs_table_name)
drop_table(cohort_w_md_uhcs_table_name)
create_table(cohort_w_md_uhcs_table_name)
optimise_table(cohort_w_md_uhcs_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_w_md_uhcs_table_name}")
print(f"`{cohort_w_md_uhcs_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

cohort_with_green_book_uhcs = formulate_uhcs("GREEN_BOOK", f"{collab_database}.{cohort_w_md_uhcs_table_name}")
print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_with_green_book_uhcs.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"""
    SELECT
        ADMISSION_IN_WINDOW,
        COUNT(*),
        100 * SUM(GREEN_BOOK_UHC) / COUNT(*) AS GREEN_BOOK_UHC_PERC,
        100 * SUM(NO_GDPPR_GREEN_BOOK_UHC) / COUNT(*) AS NO_GDPPR_GREEN_BOOK_UHC_PERC,
        100 * SUM(MD_UHC) / COUNT(*) AS MD_UHC_PERC,
        100 * SUM(NO_GDPPR_MD_UHC) / COUNT(*) AS NO_GDPPR_MD_UHC_PERC,
        100 * SUM(GREATEST(MD_UHC, GREEN_BOOK_UHC)) / COUNT(*) AS GREEN_BOOK_OR_MD_UHC_PERC,
        100 * SUM(GREATEST(NO_GDPPR_MD_UHC, NO_GDPPR_GREEN_BOOK_UHC)) / COUNT(*) AS NO_GDPPR_GREEN_BOOK_OR_MD_UHC_PERC,
        100 * SUM(GREEN_BOOK_RISK_FACTOR) / COUNT(*) AS GREEN_BOOK_RF_PERC,
        100 * SUM(NO_GDPPR_GREEN_BOOK_RISK_FACTOR) / COUNT(*) AS NO_GDPPR_GREEN_BOOK_RF_PERC,
        100 * SUM(MD_RISK_FACTOR) / COUNT(*) AS MD_RF_PERC,
        100 * SUM(NO_GDPPR_MD_RISK_FACTOR) / COUNT(*) AS NO_GDPPR_MD_RF_PERC,
        100 * SUM(GREATEST(GREEN_BOOK_RISK_FACTOR, MD_RISK_FACTOR)) / COUNT(*) AS GREEN_BOOK_OR_MD_RF_PERC,
        100 * SUM(GREATEST(NO_GDPPR_GREEN_BOOK_RISK_FACTOR, NO_GDPPR_MD_RISK_FACTOR)) / COUNT(*) AS NO_GDPPR_GREEN_BOOK_OR_MD_RF_PERC
    FROM {collab_database}.{output_table_name}
    GROUP BY ADMISSION_IN_WINDOW"""))

# COMMAND ----------


