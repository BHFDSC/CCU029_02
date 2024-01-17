# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../02/config/quiet $skip=skip

# COMMAND ----------

unpack_config(config)

# COMMAND ----------

# MAGIC %run ./utils/TABLE_NAMES

# COMMAND ----------

# MAGIC %run ./utils/ethnicity_and_sex

# COMMAND ----------

try:
  test
except:
  test = True

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

input_table_name = f"{preamble}_{cohort_w_vaccinations_output_table_name}"
output_table_name = f"{preamble}_{final_cohort_output_table_name}"

print("Adding final variables to the cohort...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_cohort_final AS
SELECT
  *,
  {generate_variant_period_case_when('ADMIDATE', variant_period_dict)} AS ADMIDATE_VARIANT_PERIOD,
  SEX AS SEX_NUMERIC,
  CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_RELATED_DEATH,
  CASE WHEN DEATH = 1 AND (PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS PIMS_RELATED_DEATH,
  CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR PIMS_PRIMARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_UNDERLYING_DEATH,
  CASE WHEN DEATH = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1 OR PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_RELATED_DEATH,
  CASE WHEN DIED_IN_HOSPITAL = 1 AND (COVID_PRIMARY_COD = 1 OR PIMS_PRIMARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_UNDERLYING_DEATH_IN_HOSPITAL,
  CASE WHEN DIED_IN_HOSPITAL = 1 AND (COVID_PRIMARY_COD = 1 OR COVID_SECONDARY_COD = 1 OR PIMS_PRIMARY_COD = 1 OR PIMS_SECONDARY_COD = 1) THEN 1 ELSE 0 END AS COVID_OR_PIMS_RELATED_DEATH_IN_HOSPITAL,
  CASE WHEN POSITIVE_TEST_IN_WINDOW = 1 OR SORT_ARRAY(ALL_POSITIVE_TESTS)[0] < ADMIDATE OR INFECTION_IDX > 1 THEN 1 ELSE 0 END AS COVID_POSITIVE_IN_WINDOW_OR_PRIOR_TO_ADMISSION,
  CASE WHEN GREEN_BOOK_RISK_FACTOR == 1 OR MD_RISK_FACTOR == 1 THEN 1 ELSE 0 END AS ANY_RISK_FACTOR,
  CASE WHEN NO_GDPPR_GREEN_BOOK_RISK_FACTOR == 1 OR NO_GDPPR_MD_RISK_FACTOR == 1 THEN 1 ELSE 0 END AS NO_GDPPR_ANY_RISK_FACTOR,
  CASE
    WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U071" THEN "U071"
    WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U072" THEN "U072"
    WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U073" THEN "U073"
    WHEN DIAG_4_CONCAT_PRIMARY RLIKE "U074" THEN "U074"
    ELSE "None"
  END AS PRIMRY_DIAG_COVID_CODE,
  CASE
    WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U071" THEN "U071"
    WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U072" THEN "U072"
    WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U073" THEN "U073"
    WHEN DIAG_4_CONCAT_SECONDARY RLIKE "U074" THEN "U074"
    ELSE "None"
  END AS SECONDARY_DIAG_COVID_CODE,
  CASE
    WHEN DIAG_4_CONCAT RLIKE "U071" THEN "U071"
    WHEN DIAG_4_CONCAT RLIKE "U072" THEN "U072"
    WHEN DIAG_4_CONCAT RLIKE "U073" THEN "U073"
    WHEN DIAG_4_CONCAT RLIKE "U074" THEN "U074"
    ELSE "None"
  END AS DIAG_COVID_CODE,
  CASE
    WHEN AGE < 1 THEN "< 1"
    WHEN AGE < 5 THEN "1 - 4"
    WHEN AGE < 12 THEN "5 - 11"
    WHEN AGE < 16 THEN "12 - 15"
    WHEN AGE < 18 THEN "16 - 17"
    ELSE "ERROR"
  END AS AGE_CAT,
  CASE
    WHEN AGE BETWEEN 2 AND 11.99999 THEN "2 - 11 (Age 2 - School Year 6)"
    WHEN AGE BETWEEN 12 AND 16.99999 THEN "12 - 16 (School Year 7 - Year 11)"
    ELSE "Remainder"
  END AS ONS_AGE_GROUP,
  CASE
    WHEN POSITIVE_TEST_IN_UKHSA_WINDOW == 1 THEN "UKHSA Type 1"
    WHEN COVID_PRIMARY_BY_CODE == 1 THEN "UKHSA Type 3"
    WHEN COVID_BY_CODE == 1 THEN "(Possible) UKHSA Type 2"
    ELSE "(Possible) UKHSA Type 4"
  END AS UKHSA_LABEL,
  CASE WHEN ETHNIC == "" THEN "9" ELSE ETHNIC END AS ETHNIC_CLEAN
FROM {collab_database}.{input_table_name}""")

# COMMAND ----------

cohort = spark.sql(f"SELECT * FROM {preamble}_cohort_final")

# Map
cohort_cleaned = cohort \
  .withColumn("ETHNIC_GROUP", mapping_expr_ethnic_group[col("ETHNIC_CLEAN")]) \
  .withColumn("ETHNICITY", mapping_expr_ethnicity[col("ETHNIC_CLEAN")]) \
  .drop("ETHNIC_CLEAN") \
  .drop("ETHNIC") \
  .withColumn("SEX", mapping_expr_sex[col("SEX")])

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_cleaned.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, 'PERSON_ID_DEID')

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

display(spark.sql(f"SELECT INFECTION_VARIANT_PERIOD, COUNT(*) FROM {collab_database}.{output_table_name} WHERE AGE BETWEEN 2 and 11.9999 GROUP BY INFECTION_VARIANT_PERIOD"))

# COMMAND ----------

display(spark.sql(f"SELECT INFECTION_VARIANT_PERIOD, COUNT(*) FROM {collab_database}.{output_table_name} WHERE AGE BETWEEN 12 and 16.9999 GROUP BY INFECTION_VARIANT_PERIOD"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS PRE_PLUS_ALPHA_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE < '2021-05-18'"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS ALPHA_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE BETWEEN '2020-12-08' AND '2021-05-17'"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS DELTA_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE BETWEEN '2021-05-18' AND '2021-12-13'"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS BA1_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE BETWEEN '2021-12-14' AND '2022-02-21'"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS BA2_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE BETWEEN '2022-02-22' AND '2022-06-06'"))

# COMMAND ----------

display(spark.sql(f"SELECT SUM(PIMS) / COUNT(*) AS BA5_IHR FROM {collab_database}.{output_table_name} WHERE INFECTION_DATE BETWEEN '2022-06-07' AND '2022-11-11'"))

# COMMAND ----------

display(spark.sql(f"SELECT DATEDIFF(ADMIDATE, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) AS ADMIDATE_MINUS_FIRST_POS_TEST_DATE FROM {collab_database}.{output_table_name} WHERE TYPE_OF_ADMISSION == 'PIMS' AND DATEDIFF(ADMIDATE, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) IS NOT NULL ORDER BY ADMIDATE_MINUS_FIRST_POS_TEST_DATE"))

# COMMAND ----------

display(spark.sql(f"""SELECT
  COUNT(*) AS COUNT,
  MEAN(ADMIDATE_MINUS_FIRST_POS_TEST_DATE) AS MEAN,
  PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.0) AS MINI,
  PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.25) AS Q1,
  PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.5) AS Q2,
  PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.75) AS Q3,
  PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 1.0) AS MAXI
FROM (
  SELECT DATEDIFF(ADMIDATE, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) AS ADMIDATE_MINUS_FIRST_POS_TEST_DATE
  FROM {collab_database}.{output_table_name}
  WHERE
    TYPE_OF_ADMISSION == 'PIMS' AND
    DATEDIFF(ADMIDATE, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) IS NOT NULL AND
    DATEDIFF(ADMIDATE, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) > 0
  ORDER BY ADMIDATE_MINUS_FIRST_POS_TEST_DATE
)"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
  DATEDIFF(PIMS_EPISTART, FIRST_POSITIVE_TEST_DATE) AS ADMIDATE_MINUS_FIRST_POS_TEST_DATE
  FROM {collab_database}.{output_table_name} a
  LEFT JOIN (
    SELECT PERSON_ID_DEID, MIN(Specimen_Date) AS FIRST_POSITIVE_TEST_DATE
    FROM {collab_database}.{preamble}_sgss
    WHERE Specimen_Date IS NOT NULL
    GROUP BY PERSON_ID_DEID
  ) b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID
  WHERE PIMS == 1 AND DATEDIFF(PIMS_EPISTART, FIRST_POSITIVE_TEST_DATE) IS NOT NULL AND DATEDIFF(PIMS_EPISTART, FIRST_POSITIVE_TEST_DATE) > 5
  ORDER BY ADMIDATE_MINUS_FIRST_POS_TEST_DATE"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) AS COUNT, MEAN(ADMIDATE_MINUS_FIRST_POS_TEST_DATE) AS MEAN, PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.0) AS MINI, PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.25) AS Q1, PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.5) AS Q2, PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 0.75) AS Q3, PERCENTILE(ADMIDATE_MINUS_FIRST_POS_TEST_DATE, 1.0) AS MAXI
FROM (
  SELECT
    DATEDIFF(PIMS_EPISTART, FIRST_POSITIVE_TEST_DATE) AS ADMIDATE_MINUS_FIRST_POS_TEST_DATE
    FROM {collab_database}.{output_table_name} a
    LEFT JOIN (
      SELECT PERSON_ID_DEID, MIN(Specimen_Date) AS FIRST_POSITIVE_TEST_DATE
      FROM {collab_database}.{preamble}_sgss
      WHERE Specimen_Date is NOT NULL
      GROUP BY PERSON_ID_DEID
    ) b
    ON a.PERSON_ID_DEID == b.PERSON_ID_DEID
    WHERE 
      PIMS == 1 AND
      DATEDIFF(PIMS_EPISTART, FIRST_POSITIVE_TEST_DATE) IS NOT NULL AND
      DATEDIFF(PIMS_EPISTART, SORT_ARRAY(ALL_POSITIVE_TESTS)[0]) > 0
    ORDER BY ADMIDATE_MINUS_FIRST_POS_TEST_DATE
)"""))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT TYPE_OF_ADMISSION, COUNT(*), SUM(COVID_BY_TEST), (SUM(COVID_BY_TEST) / COUNT(*)) * 100 FROM {collab_database}.{output_table_name} GROUP BY TYPE_OF_ADMISSION"))

# COMMAND ----------

if test:
  display(spark.sql(f"""SELECT CASE WHEN TYPE_OF_ADMISSION RLIKE 'Type A' THEN 'Type A' WHEN TYPE_OF_ADMISSION RLIKE 'Type B' THEN 'Type B' ELSE TYPE_OF_ADMISSION END, 100 * SUM(CASE WHEN COVID_BY_TEST == 1 THEN 1 ELSE 0 END) / COUNT(*) FROM {collab_database}.{output_table_name} GROUP BY CASE WHEN TYPE_OF_ADMISSION RLIKE 'Type A' THEN 'Type A' WHEN TYPE_OF_ADMISSION RLIKE 'Type B' THEN 'Type B' ELSE TYPE_OF_ADMISSION END"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""SELECT
  SUM(CASE WHEN ADMISSION_IN_WINDOW == 1 AND GREEN_BOOK_UHC == 1 THEN 1 ELSE 0 END) AS NUM_UHC,
  SUM(CASE WHEN ADMISSION_IN_WINDOW == 1 AND GREEN_BOOK_UHC == 0 THEN 1 ELSE 0 END) AS NUM_NON_UHC,
  SUM(CASE WHEN ADMISSION_IN_WINDOW == 1 THEN 1 ELSE 0 END) AS NUM_ADMISSIONS
FROM {collab_database}.{output_table_name} WHERE AGE BETWEEN 5 AND 11.99999"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""SELECT
  ROUND(100 * SUM(CASE WHEN COVID_OR_PIMS_RELATED_DEATH + ADMISSION_IN_WINDOW + MD_UHC == 3 THEN 1 ELSE 0 END) / SUM(CASE WHEN COVID_OR_PIMS_RELATED_DEATH + ADMISSION_IN_WINDOW == 2 THEN 1 ELSE 0 END), 1) AS PERC_MD_UHC_IN_CVD_AND_PIMS_RELATED_DEATHS_ADMISSIONS,
  ROUND(100 * SUM(CASE WHEN COVID_OR_PIMS_RELATED_DEATH + MD_UHC == 2 THEN 1 ELSE 0 END) / SUM(CASE WHEN COVID_OR_PIMS_RELATED_DEATH == 1 THEN 1 ELSE 0 END), 1) AS PERC_MD_UHC_IN_CVD_AND_PIMS_RELATED_DEATHS_TESTS_ONLY
FROM {collab_database}.{output_table_name}"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  SUM(CASE WHEN ICU == 1 AND ADMIDATE BETWEEN '{datetime.strftime(datetime.strptime(study_end, '%Y-%m-%d') - timedelta(days=365), '%Y-%m-%d')}' AND '{study_end}' THEN 1 ELSE 0 END) AS N_ICU_IN_YEAR_FROM_STUDY_END,
  SUM(CASE WHEN ICU == 1 AND TYPE_OF_ADMISSION == 'Incidental' AND ADMIDATE BETWEEN '{datetime.strftime(datetime.strptime(study_end, '%Y-%m-%d') - timedelta(days=365), '%Y-%m-%d')}' AND '{study_end}' THEN 1 ELSE 0 END) AS N_INCIDENTAL_ICU_IN_YEAR_FROM_STUDY_END,
  SUM(CASE WHEN ICU == 1 AND ADMIDATE BETWEEN '{datetime.strftime(datetime.strptime(infection_censoring_date, '%Y-%m-%d') - timedelta(days=365), '%Y-%m-%d')}' AND '{infection_censoring_date}' THEN 1 ELSE 0 END) AS N_ICU_IN_YEAR_FROM_INFECTION_CENSORING
FROM {collab_database}.{output_table_name}"""))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT CASE WHEN ADMISSION_IN_WINDOW == 1 THEN 'COVID Admission' ELSE 'Positive Test Only' END AS GROUP, ROUND(100 * SUM(CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_WEIGHT_PRESENT, ROUND(100 * SUM(CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_HEIGHT_PRESENT, ROUND(100 * SUM(CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_BMI_PRESENT FROM {collab_database}.{output_table_name} GROUP BY CASE WHEN ADMISSION_IN_WINDOW == 1 THEN 'COVID Admission' ELSE 'Positive Test Only' END"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT SUM(CASE WHEN ICU == 1 OR TYPE_OF_ADMISSION == 'PIMS' THEN 1 ELSE 0 END) FROM {collab_database}.{output_table_name}"))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  ADMISSION_IN_WINDOW,
  COUNT(*),
  SUM(CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN BMI IS NOT NULL AND WEIGHT IS NOT NULL AND HEIGHT IS NOT NULL THEN 1 ELSE 0 END)
FROM {collab_database}.{output_table_name}
GROUP BY ADMISSION_IN_WINDOW"""))

# COMMAND ----------

if test:
  uhc_names_1 = ',\n  '.join(list(dict.fromkeys([f"SUM(CASE WHEN UHC == '{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}' THEN 1 ELSE 0 END) AS N_{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x or "PRESENT_CODES_FIVE_YEARS_GREEN_BOOK" in x])))
  print(uhc_names_1)
  uhc_names_2 = '\n  UNION ALL\n  '.join([f"SELECT EXPLODE(ARRAY_DISTINCT(SPLIT({x}, ','))) AS ICD10_CODE, '{x.replace('PRESENT_CODES_GREEN_BOOK_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_GREEN_BOOK_UHC_', '')}' AS UHC\n  FROM {collab_database}.{output_table_name}" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x or "PRESENT_CODES_FIVE_YEARS_GREEN_BOOK" in x])
  print(uhc_names_2)
  
  separate_UHC_code_counts = spark.sql(f"""
SELECT
  {uhc_names_1},
  ICD10_CODE,
  ICD10_DESCRIPTION,
  DESCRIPTIONS_ABBREVIATED,
  ICD10_CHAPTER_HEADING,
  ICD10_CHAPTER_DESCRIPTION,
  ICD10_GROUP_HEADING,
  ICD10_GROUP_DESCRIPTION
FROM (
  {uhc_names_2}
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY ICD10_CODE""")
  
  table_name = f"{preamble}_separate_green_book_uhc_code_counts"
  
  print(f"Creating `{table_name}` with study start date == {study_start}")

  separate_UHC_code_counts.createOrReplaceTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

# COMMAND ----------

if test:
  uhc_names_1 = ',\n  '.join(list(dict.fromkeys([f"SUM(CASE WHEN UHC == '{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}' THEN 1 ELSE 0 END) AS N_{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}" for x in table.schema.names if "PRESENT_CODES_MD" in x or "PRESENT_CODES_FIVE_YEARS_MD" in x])))
  print(uhc_names_1)
  uhc_names_2 = '\n  UNION ALL\n  '.join([f"SELECT EXPLODE(ARRAY_DISTINCT(SPLIT({x}, ','))) AS ICD10_CODE, '{x.replace('PRESENT_CODES_MD_UHC_', '').replace('PRESENT_CODES_FIVE_YEARS_MD_UHC_', '')}' AS UHC\n  FROM {collab_database}.{output_table_name}" for x in table.schema.names if "PRESENT_CODES_MD" in x or "PRESENT_CODES_FIVE_YEARS_MD" in x])
  print(uhc_names_2)
  
  separate_UHC_code_counts = spark.sql(f"""
SELECT
  {uhc_names_1},
  ICD10_CODE,
  ICD10_DESCRIPTION,
  DESCRIPTIONS_ABBREVIATED,
  ICD10_CHAPTER_HEADING,
  ICD10_CHAPTER_DESCRIPTION,
  ICD10_GROUP_HEADING,
  ICD10_GROUP_DESCRIPTION
FROM (
  {uhc_names_2}
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY ICD10_CODE""")
  
  table_name = f"{preamble}_separate_md_uhc_code_counts"
  
  print(f"Creating `{table_name}` with study start date == {study_start}")

  separate_UHC_code_counts.createOrReplaceTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  SUM(CASE WHEN ADMIDATE >= '2021-10-01' AND UKHSA_LABEL RLIKE "^UKHSA Type" THEN 1 ELSE 0 END) AS UKHSA_PERIOD_1,
  SUM(CASE WHEN ADMIDATE BETWEEN '2021-04-07' AND '2021-09-30' AND UKHSA_LABEL RLIKE "^UKHSA Type" THEN 1 ELSE 0 END) AS UKHSA_PERIOD_2,
  SUM(CASE WHEN ADMIDATE BETWEEN '2020-10-12' AND '2021-04-06' AND UKHSA_LABEL RLIKE "^UKHSA Type" THEN 1 ELSE 0 END) AS UKHSA_PERIOD_3
FROM {collab_database}.{output_table_name}"""))

# COMMAND ----------

if test:
  display(spark.sql(f'''
SELECT
  COUNT(*),
  INFECTION_IDX,
  VACCINATION_STATUS_14_DAYS_PRIOR_TO_INFECTION
FROM {collab_database}.{output_table_name}
GROUP BY VACCINATION_STATUS_14_DAYS_PRIOR_TO_INFECTION, INFECTION_IDX
ORDER BY INFECTION_IDX, VACCINATION_STATUS_14_DAYS_PRIOR_TO_INFECTION'''))

# COMMAND ----------

if test:
  code_col_names = ", ".join([f"SPLIT({x}, ',')" for x in table.schema.names if "PRESENT_CODES_GREEN_BOOK" in x])
  print(code_col_names)

# COMMAND ----------

if test:
  PIMS_UHC_code_counts = spark.sql(f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(FLATTEN(COLLECT_LIST(FLATTEN(ARRAY_DISTINCT(ARRAY({code_col_names})))))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE GREEN_BOOK_RISK_FACTOR = 1 AND TYPE_OF_ADMISSION = 'PIMS'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC""")
  
  table_name = f"{preamble}_pims_uhc_code_counts"
  
  print(f"Creating `{table_name}` with study start date == {study_start}")

  PIMS_UHC_code_counts.createOrReplaceTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

  table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
  print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")
  
  display(table)

# COMMAND ----------

if test:
  PIMS_UHC_code_counts = spark.sql(f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(FLATTEN(COLLECT_LIST(FLATTEN(ARRAY_DISTINCT(ARRAY({code_col_names})))))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE GREEN_BOOK_RISK_FACTOR = 1
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC""")
  
  table_name = f"{preamble}_uhc_code_counts"
  
  print(f"Creating `{table_name}` with study start date == {study_start}")

  PIMS_UHC_code_counts.createOrReplaceTempView(table_name)
  drop_table(table_name)
  create_table(table_name)

  table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
  print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")
  
  display(table)

# COMMAND ----------

# Kate requesting monthly counts of PIMS

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT DAY_CASE, COUNT(*) FROM {collab_database}.{output_table_name} GROUP BY DAY_CASE"))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT COUNT(*)
FROM {collab_database}.{output_table_name} a
INNER JOIN {collab_database}.curr302_patient_skinny_record b
ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  TYPE_OF_ADMISSION,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'U071' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_U071,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'U072' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_U072,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'U073' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_U073,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'U074' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_U074,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT NOT RLIKE 'U071|U072|U073|U074' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_NONE  
FROM {collab_database}.{output_table_name}
WHERE ADMISSION_IN_WINDOW == 1
GROUP BY TYPE_OF_ADMISSION
ORDER BY TYPE_OF_ADMISSION"""))

# COMMAND ----------

if test:
  display(spark.sql(f"""
SELECT
  TYPE_OF_ADMISSION,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'U075' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_U075,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'M303' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_M303,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT RLIKE 'R65' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_R65,
  ROUND(100 * SUM(CASE WHEN DIAG_4_CONCAT NOT RLIKE 'R65|M303|U075' THEN 1 ELSE 0 END) / COUNT(*), 1) AS PERC_NONE  
FROM {collab_database}.{output_table_name}
WHERE TYPE_OF_ADMISSION NOT RLIKE 'Other|Exclude' AND TYPE_OF_ADMISSION IS NOT NULL
GROUP BY TYPE_OF_ADMISSION ORDER BY TYPE_OF_ADMISSION"""))
