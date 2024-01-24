# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../config/quiet $skip=skip

# COMMAND ----------

unpack_config(config)

# COMMAND ----------

# MAGIC %run ./utils/TABLE_NAMES

# COMMAND ----------

try:
    test
except:
    test = True

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

output_table_name = f"{preamble}_{tests_output_table_name}"

print("Creating consistent and filtered version of the SGSS testing table (under 18s at study start, known sex)...")

# COMMAND ----------

# Again, standardise characteristics of records at a per-person level, then take the first positive test for each person in SGSS

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_skinny_dob_per_person AS
SELECT
  a.PERSON_ID_DEID,
  COALESCE(GREATEST(COALESCE(b.DATE_OF_BIRTH, c.DATE_OF_BIRTH), COALESCE(c.DATE_OF_BIRTH, b.DATE_OF_BIRTH)), a.DATE_OF_BIRTH) AS DATE_OF_BIRTH
FROM (
  SELECT
    PERSON_ID_DEID,
    MAX(DOB) AS DATE_OF_BIRTH
  FROM {collab_database}.{preamble}_skinny
  GROUP BY PERSON_ID_DEID
) a
LEFT JOIN (
  SELECT
    PERSON_ID_DEID,
    MAX(to_date(MYDOB, "MMyyyy")) AS DATE_OF_BIRTH
  FROM {collab_database}.{preamble}_hes_apc
  GROUP BY PERSON_ID_DEID
) b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
LEFT JOIN (
  SELECT
    NHS_NUMBER_DEID,
    MAX(to_date(YEAR_MONTH_OF_BIRTH, "yyyy-MM")) AS DATE_OF_BIRTH
  FROM {collab_database}.{preamble}_gdppr
  GROUP BY NHS_NUMBER_DEID
) c
ON a.PERSON_ID_DEID = c.NHS_NUMBER_DEID"""
)
spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_skinny_per_person AS
SELECT
  a.*,
  b.DATE_OF_BIRTH
FROM (
  SELECT
    PERSON_ID_DEID,
    FIRST(ETHNIC, TRUE) AS ETHNIC,
    FIRST(SEX, TRUE) AS SEX,
    FIRST(LSOA, TRUE) AS LSOA
  FROM (
    SELECT *
    FROM {collab_database}.{preamble}_skinny
    WHERE SEX IN (1,2) AND LSOA LIKE 'E%'
    DISTRIBUTE BY PERSON_ID_DEID
    SORT BY PERSON_ID_DEID, ETHNIC, SEX, LSOA
  )
  GROUP BY PERSON_ID_DEID
) a
LEFT JOIN {preamble}_skinny_dob_per_person b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_positive_test_dates AS
SELECT DISTINCT
  PERSON_ID_DEID,
  Specimen_Date AS POSITIVE_TEST_DATE
FROM {collab_database}.{preamble}_sgss
WHERE Specimen_Date IS NOT NULL"""
)

# COMMAND ----------

full_cohort = spark.sql(
    f"""
SELECT
  a.*,
  POSITIVE_TEST_DATE,
  DATEDIFF(POSITIVE_TEST_DATE, DATE_OF_BIRTH) / 365.25 AS POSITIVE_TEST_AGE,
  {generate_variant_period_case_when('POSITIVE_TEST_DATE', variant_period_dict)} AS POSITIVE_TEST_VARIANT_PERIOD,
  DECI_IMD
FROM {preamble}_skinny_per_person a
INNER JOIN {preamble}_positive_test_dates b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
LEFT JOIN (
  SELECT
    FIRST(DECI_IMD, TRUE) AS DECI_IMD,
    LSOA_CODE_2011
  FROM (
    SELECT *
    FROM dss_corporate.english_indices_of_dep_v02
    DISTRIBUTE BY LSOA_CODE_2011
    SORT BY LSOA_CODE_2011, IMD_YEAR DESC
  )
  GROUP BY LSOA_CODE_2011
) c
ON a.LSOA = c.LSOA_CODE_2011
WHERE POSITIVE_TEST_DATE <= '{study_end}'"""
)

# This is study end date such that tests can be linked after, but then the infection censoring at the next step ensures infections can only begin 42 days prior

print(f"Creating `{output_table_name}` with study start date == {study_start}")

full_cohort.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
