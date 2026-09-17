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

hes_apc_demographics_table_name = f"{preamble}_{hes_apc_demographics_table_name}"
admissions_output_table_name = f"{preamble}_{admissions_output_table_name}"

# COMMAND ----------

# The below is fairly self explanatory, we must assign consistent birth dates, ethnicity, LSOA and sex to each individual represented in the data, and a consistent discharge date for each person-admission pair, bias towards records that assign people known sex, in England, and are likely to have valid dates, as many records in HES have non-NULL dates that are far in the past as some NULL equivalent, but are clearly false e.g. 1800-01-01

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_date_of_birth_fixed AS
SELECT
  a.PERSON_ID_DEID,
  COALESCE(a.DATE_OF_BIRTH, b.DATE_OF_BIRTH, c.DATE_OF_BIRTH) AS DATE_OF_BIRTH
FROM (
  SELECT
    PERSON_ID_DEID,
    MAX(to_date(MYDOB, "MMyyyy")) AS DATE_OF_BIRTH
  FROM {collab_database}.{preamble}_hes_apc
  GROUP BY PERSON_ID_DEID
) a
LEFT JOIN (
  SELECT
    PERSON_ID_DEID,
    MAX(DOB) AS DATE_OF_BIRTH
  FROM {collab_database}.{preamble}_skinny
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
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_disdate_fixed AS
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  MAX(DISDATE) AS DISDATE
FROM {collab_database}.{preamble}_hes_apc
GROUP BY PERSON_ID_DEID, ADMIDATE"""
)
spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_fixed_per_person_demographics AS
SELECT
  PERSON_ID_DEID,
  FIRST(LSOA, TRUE) AS LSOA,
  FIRST(ETHNIC, TRUE) AS ETHNIC,
  FIRST(SEX, TRUE) AS SEX,
  FIRST(TRUST_CODE, TRUE) AS TRUST_CODE
FROM (
  SELECT
    a.PERSON_ID_DEID,
    a.ADMIDATE,
    COALESCE(b.LSOA, a.LSOA11) AS LSOA,
    COALESCE(b.ETHNIC, a.ETHNOS) AS ETHNIC,
    COALESCE(b.SEX, a.SEX) AS SEX,
    a.PROCODE3 AS TRUST_CODE
  FROM (SELECT DISTINCT PERSON_ID_DEID, ADMIDATE, LSOA11, ETHNOS, SEX, PROCODE3 FROM {collab_database}.{preamble}_hes_apc) a
  LEFT JOIN {collab_database}.{preamble}_skinny b
  ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
  WHERE COALESCE(b.LSOA, a.LSOA11) LIKE "E%" AND COALESCE(b.SEX, a.SEX) IN (1,2)
  DISTRIBUTE BY a.PERSON_ID_DEID
  SORT BY a.PERSON_ID_DEID, a.ADMIDATE DESC, COALESCE(b.LSOA, a.LSOA11), COALESCE(b.ETHNIC, a.ETHNOS), COALESCE(b.SEX, a.SEX), TRUST_CODE
)
GROUP BY PERSON_ID_DEID"""
)
spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_fixed_demographics AS
SELECT
  a.PERSON_ID_DEID,
  a.ADMIDATE,
  CASE WHEN a.DISDATE > '{study_end}' THEN NULL ELSE a.DISDATE END AS DISDATE,
  b.LSOA,
  b.ETHNIC,
  b.SEX,
  c.DATE_OF_BIRTH,
  DATEDIFF(a.ADMIDATE, c.DATE_OF_BIRTH) / 365.25 AS EVENT_AGE,
  b.TRUST_CODE
FROM {preamble}_hes_apc_disdate_fixed a
INNER JOIN {preamble}_hes_apc_fixed_per_person_demographics b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
INNER JOIN {preamble}_hes_apc_date_of_birth_fixed c
ON a.PERSON_ID_DEID = c.PERSON_ID_DEID"""
)

# COMMAND ----------

print(
    f"Filtering HES APC to valid records occurring before the end of the study period ({study_end}) and after a reasonable start date ({hospitalisation_start_date}) for the pandemic..."
)

# COMMAND ----------

hes_apc_fixed_censored = spark.sql(
    f"""
SELECT
  *, DATEDIFF(DISDATE, ADMIDATE) + 1 as LENGTH_OF_STAY
FROM {preamble}_hes_apc_fixed_demographics
WHERE
  ADMIDATE {"BETWEEN '" + hospitalisation_start_date + "' AND '" + study_end + "'" if hospitalisation_start_date else "<= '" + study_end + "'"}
  AND (ADMIDATE <= DISDATE OR DISDATE IS NULL)
"""
)

print(f"Creating `{hes_apc_demographics_table_name}` with study start date == {study_start}...")

hes_apc_fixed_censored.createOrReplaceTempView(hes_apc_demographics_table_name)
drop_table(hes_apc_demographics_table_name)
create_table(hes_apc_demographics_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{hes_apc_demographics_table_name}")
print(f"`{hes_apc_demographics_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
print("Collapsing records into individual spells...")

# COMMAND ----------

# We group by all of the characteristics standardised above to convert HES APC which is an episode-level dataset, into a spell-level one, and concatenate and collect all of the codes for each episode in a spell into a single record

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_annotated_episodes AS
SELECT
  *,
  CASE WHEN DIAG_4_01 RLIKE '{primary_codes}' THEN 1 ELSE 0 END AS COVID_PRIMARY_EPI_BY_CODE,
  CASE WHEN DIAG_4_CONCAT RLIKE 'U071|U072' THEN 1 ELSE 0 END AS COVID_EPI_BY_CODE,
  CASE WHEN (ADMIDATE BETWEEN '{pims_defn_date}' AND '2021-12-13' AND (DIAG_4_01 RLIKE 'U075' OR (DIAG_4_01 RLIKE 'M303|R65' AND DIAG_4_CONCAT NOT RLIKE '{pims_reject_code_statement}'))) OR (ADMIDATE > '2021-12-13' AND DIAG_4_01 RLIKE 'U075') THEN 1 ELSE 0 END AS PIMS_PRIMARY_EPI_BY_CODE,
  CASE WHEN (ADMIDATE BETWEEN '{pims_defn_date}' AND '2021-12-13' AND (DIAG_4_CONCAT RLIKE 'U075' OR (DIAG_4_CONCAT RLIKE 'M303|R65' AND DIAG_4_CONCAT NOT RLIKE '{pims_reject_code_statement}'))) OR (ADMIDATE > '2021-12-13' AND DIAG_4_CONCAT RLIKE 'U075') THEN 1 ELSE 0 END AS PIMS_EPI_BY_CODE,
  CASE WHEN ADMIDATE >= '{pims_defn_date}' AND (DIAG_4_01 RLIKE 'U075' OR (DIAG_4_01 RLIKE 'M303|R65' AND DIAG_4_CONCAT NOT RLIKE '{pims_reject_code_statement}')) THEN 1 ELSE 0 END AS PIMS_PRIMARY_EPI_BY_CODE_OLD,
  CASE WHEN ADMIDATE >= '{pims_defn_date}' AND (DIAG_4_CONCAT RLIKE 'U075' OR (DIAG_4_CONCAT RLIKE 'M303|R65' AND DIAG_4_CONCAT NOT RLIKE '{pims_reject_code_statement}')) THEN 1 ELSE 0 END AS PIMS_EPI_BY_CODE_OLD,
  NULLIF(CONCAT_WS(',', DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, DIAG_4_19, DIAG_4_20), '') AS DIAG_4_SECONDARY
FROM {collab_database}.{preamble}_hes_apc"""
)

# COMMAND ----------

# 1st infection - test based 2022-01-01
# 2nd infection - admission based non-primary (no other covid evidence) 2022-05-01 NOT VALID

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_filtered_spells_raw AS
SELECT
  a.*,
  COUNT(*) AS EPICOUNT,
  CONCAT_WS(',', COLLECT_LIST(COALESCE(EPISTART, '?'))) AS ALL_EPISTARTS,
  CONCAT_WS(',', COLLECT_LIST(COALESCE(EPIORDER, '?'))) AS ALL_EPIORDERS,
  MIN(
    CASE
      WHEN COVID_EPI_BY_CODE > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS COVID_EPISTART,
  MIN(
    CASE
      WHEN COVID_PRIMARY_EPI_BY_CODE > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS COVID_PRIMARY_EPISTART,
  MIN(
    CASE
      WHEN PIMS_EPI_BY_CODE > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS PIMS_EPISTART,
    MIN(
    CASE
      WHEN PIMS_PRIMARY_EPI_BY_CODE > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS PIMS_PRIMARY_EPISTART,
  MIN(
    CASE
      WHEN PIMS_EPI_BY_CODE_OLD > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS PIMS_EPISTART_OLD,
    MIN(
    CASE
      WHEN PIMS_PRIMARY_EPI_BY_CODE_OLD > 0
      THEN COALESCE(EPISTART, a.ADMIDATE)
      ELSE NULL END
  ) AS PIMS_PRIMARY_EPISTART_OLD,
  CASE WHEN SUM(COVID_EPI_BY_CODE) > 0 THEN 1 ELSE 0 END AS COVID_BY_CODE,
  CASE WHEN SUM(COVID_PRIMARY_EPI_BY_CODE) > 0 THEN 1 ELSE 0 END AS COVID_PRIMARY_BY_CODE,
  CASE WHEN SUM(PIMS_EPI_BY_CODE) > 0 THEN 1 ELSE 0 END AS PIMS_BY_CODE,
  CASE WHEN SUM(PIMS_PRIMARY_EPI_BY_CODE) > 0 THEN 1 ELSE 0 END AS PIMS_PRIMARY_BY_CODE,
  CASE WHEN SUM(PIMS_EPI_BY_CODE_OLD) > 0 THEN 1 ELSE 0 END AS PIMS_BY_CODE_OLD,
  CASE WHEN SUM(PIMS_PRIMARY_EPI_BY_CODE_OLD) > 0 THEN 1 ELSE 0 END AS PIMS_PRIMARY_BY_CODE_OLD,
  CASE WHEN a.DISDATE IS NULL THEN 1 ELSE 0 END AS STILL_IN_HOSPITAL,
  CASE WHEN SUM(CASE WHEN CLASSPAT = 2 THEN 1 ELSE 0 END) >= 1 AND a.LENGTH_OF_STAY == 1 THEN 1 ELSE 0 END AS DAY_CASE,
  CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_CONCAT, ',')))) AS DIAG_4_CONCAT,
  CONCAT_WS(',', FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_SECONDARY, ',')))) AS DIAG_4_CONCAT_SECONDARY,
  CONCAT_WS(',', COLLECT_LIST(DIAG_4_01)) AS DIAG_4_CONCAT_PRIMARY,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_CONCAT, ','))))) AS UNIQUE_DIAG_4_CONCAT,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(DIAG_4_SECONDARY, ','))))) AS UNIQUE_DIAG_4_CONCAT_SECONDARY,
  CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(DIAG_4_01))) AS UNIQUE_DIAG_4_CONCAT_PRIMARY,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(OPERTN_3_CONCAT, '-'), ','))))) AS OPERTN_3_CONCAT,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(OPERTN_4_CONCAT, '-'), ','))))) AS OPERTN_4_CONCAT
FROM {collab_database}.{hes_apc_demographics_table_name} a
INNER JOIN (SELECT * FROM {preamble}_hes_apc_annotated_episodes DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, EPIORDER ASC) b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.ADMIDATE = b.ADMIDATE
GROUP BY a.PERSON_ID_DEID, a.ADMIDATE, a.DISDATE, a.DATE_OF_BIRTH, a.EVENT_AGE, a.SEX, a.ETHNIC, a.LSOA, a.TRUST_CODE, a.LENGTH_OF_STAY"""
)

# COMMAND ----------

# After converting to a spell-level dataset, calculate and derive a number of other variables using the newly collected spell-level information
finished_spells = spark.sql(
    f"""
SELECT
  a.*,
  -- OPERATIONAL CODES Presence of CPAP/NIV/IMV implies Oxygen:
  CASE WHEN OPERTN_3_CONCAT RLIKE 'X52|E856|E852|E851' THEN 1 ELSE 0 END AS PRESENCE_OXYGEN,
  CASE WHEN OPERTN_4_CONCAT LIKE '%E856%' THEN 1 ELSE 0 END AS PRESENCE_CPAP,
  CASE WHEN OPERTN_4_CONCAT LIKE '%E852%' THEN 1 ELSE 0 END AS PRESENCE_NIV,
  CASE WHEN OPERTN_4_CONCAT LIKE '%E851%' THEN 1 ELSE 0 END AS PRESENCE_IMV,
  CASE WHEN OPERTN_4_CONCAT LIKE '%X581%' THEN 1 ELSE 0 END AS PRESENCE_ECMO,
  b.DECI_IMD AS DECI_IMD
FROM {preamble}_hes_apc_filtered_spells_raw a
-- The join to IMD data is done like this because there are multiple rows per LSOA_CODE_2011, we order by IMD_YEAR DESC and take the first DECI_IMD value to hopefully get the most recent
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
) b
ON a.LSOA = b.LSOA_CODE_2011"""
)

print(f"Creating `{admissions_output_table_name}` with study start date == {study_start}")

finished_spells.createOrReplaceTempView(admissions_output_table_name)
drop_table(admissions_output_table_name)
create_table(admissions_output_table_name)

optimise_table(admissions_output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{admissions_output_table_name}")
print(f"`{admissions_output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
