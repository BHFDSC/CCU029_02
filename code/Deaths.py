# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../01/config/quiet $skip=skip

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

input_table_name = f"{preamble}_{cohort_w_all_uhcs_output_table_name}"
output_table_name = f"{preamble}_{cohort_w_deaths_output_table_name}"

print("Running tests on deaths data...")

# COMMAND ----------

# # Deaths

# According to the documentation, here `DEC_CONF_NHS_NUMBER_CLEAN_DEID` is the patient identifier column, there are a large number of duplicate IDs in the table. 

# The following few queries illustrate some data quality issues, eventually we just aggregate at a patient ID level and return concatenated COD in primary and non-primary positions for all records that are then joined via this ID to our cohort.

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  COUNT(*) as N,
  COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) as COUNT_DISTINCT_ID,
  COUNT(*) - COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) as COUNT_DUPLICATE_ID
FROM
  {collab_database}.{preamble}_deaths
WHERE
  -- Needed for QC
  to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= '{study_end}'"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT COUNT(DISTINCT PERSON_ID_DEID), COUNT(*) FROM (
  SELECT
    DEC_CONF_NHS_NUMBER_CLEAN_DEID as PERSON_ID_DEID,
    MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) as REG_DATE_OF_DEATH
  FROM {collab_database}.{preamble}_deaths
  WHERE
    DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
    AND to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') <= '{study_end}'
  GROUP BY PERSON_ID_DEID
)"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT SUM(N) - COUNT(*) AS NUMBER_OF_REPEAT_PATIENT_RECORDS_ON_SAME_DOD
FROM (
  SELECT
    a.DEC_CONF_NHS_NUMBER_CLEAN_DEID,
    COUNT(a.DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N
  FROM {collab_database}.{preamble}_deaths a
  INNER JOIN (
    SELECT
      DEC_CONF_NHS_NUMBER_CLEAN_DEID,
      MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) as REG_DATE_OF_DEATH
    FROM {collab_database}.{preamble}_deaths
    WHERE
      DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
      AND to_date(REG_DATE_OF_DEATH,'yyyyMMdd') <= '{study_end}'
    GROUP BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
  ) b
  ON 
    a.DEC_CONF_NHS_NUMBER_CLEAN_DEID = b.DEC_CONF_NHS_NUMBER_CLEAN_DEID
    AND to_date(a.REG_DATE_OF_DEATH, 'yyyyMMdd') = b.REG_DATE_OF_DEATH
  GROUP BY a.DEC_CONF_NHS_NUMBER_CLEAN_DEID
  HAVING COUNT(a.DEC_CONF_NHS_NUMBER_CLEAN_DEID) > 1
)"""))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_deaths_to_join AS
SELECT
  1 AS DEATH,
  DEC_CONF_NHS_NUMBER_CLEAN_DEID AS PERSON_ID_DEID,
  MIN(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd')) AS REG_DATE_OF_DEATH,
  CONCAT_WS(',', COLLECT_LIST(to_date(REG_DATE_OF_DEATH, 'yyyyMMdd'))) AS ALL_REG_DATE_OF_DEATH_debug,
  COUNT(*) AS RECORD_COUNT,
  NULLIF(CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ''), ',')))), '') AS COD_CODES,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ''), ','))))), '') AS UNIQUE_COD_CODES,
  NULLIF(CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', ARRAY_EXCEPT(ARRAY(S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ARRAY(S_UNDERLYING_COD_ICD10))), ''), ',')))), '') AS COD_CODES_SECONDARY,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(CONCAT_WS(',', ARRAY_EXCEPT(ARRAY(S_COD_CODE_1, S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8, S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15), ARRAY(S_UNDERLYING_COD_ICD10))), ''), ','))))), '') AS UNIQUE_COD_CODES_SECONDARY,
  NULLIF(CONCAT_WS(',', COLLECT_LIST(NULLIF(S_UNDERLYING_COD_ICD10, ''))), '') AS COD_CODES_PRIMARY,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(NULLIF(S_UNDERLYING_COD_ICD10, '')))), '') AS UNIQUE_COD_CODES_PRIMARY
FROM {collab_database}.{preamble}_deaths
WHERE
  DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
  AND to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') <= '{study_end}'
GROUP BY PERSON_ID_DEID""")

# COMMAND ----------

print("Joining dates and causes of death to the cohort...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_cohort_w_deaths AS
SELECT
  a.*,
  COALESCE(DEATH, 0) as DEATH,
  REG_DATE_OF_DEATH,
  ALL_REG_DATE_OF_DEATH_debug,
  CASE WHEN REG_DATE_OF_DEATH BETWEEN a.ADMIDATE AND a.DISDATE THEN 1 ELSE 0 END AS DIED_IN_HOSPITAL,
  RECORD_COUNT,
  CASE WHEN COD_CODES_PRIMARY RLIKE '{primary_codes}' THEN 1 ELSE 0 END AS COVID_PRIMARY_COD, 
  CASE WHEN COD_CODES_SECONDARY RLIKE 'U071|U072' THEN 1 ELSE 0 END AS COVID_SECONDARY_COD,
  CASE WHEN INFECTION_DATE >= '{pims_defn_date}' AND (COD_CODES_PRIMARY RLIKE 'U075' OR (COD_CODES_PRIMARY RLIKE 'M303|R65' AND COD_CODES NOT RLIKE '{pims_reject_code_statement}')) THEN 1 ELSE 0 END AS PIMS_PRIMARY_COD,
  CASE WHEN INFECTION_DATE >= '{pims_defn_date}' AND (COD_CODES_SECONDARY RLIKE 'U075' OR (COD_CODES_SECONDARY RLIKE 'M303|R65' AND COD_CODES NOT RLIKE '{pims_reject_code_statement}')) THEN 1 ELSE 0 END AS PIMS_SECONDARY_COD,
  COD_CODES,
  UNIQUE_COD_CODES,
  COD_CODES_SECONDARY,
  UNIQUE_COD_CODES_SECONDARY,
  COD_CODES_PRIMARY,
  UNIQUE_COD_CODES_PRIMARY
FROM {collab_database}.{input_table_name} a
LEFT JOIN {preamble}_deaths_to_join b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID""")

# COMMAND ----------

cohort_joined = spark.sql(f"SELECT * FROM {preamble}_cohort_w_deaths")

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_joined.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*) FROM {collab_database}.{output_table_name}"))
