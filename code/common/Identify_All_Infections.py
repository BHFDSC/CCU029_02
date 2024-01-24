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

tests_table_name = f"{preamble}_{tests_output_table_name}"
# dbutils.widgets.text("tests_table_name", tests_table_name)
admissions_table_name = f"{preamble}_{admissions_typed_output_table_name}"
# dbutils.widgets.text("admissions_table_name", admissions_table_name)
all_covid_admissions_table_name = f"{preamble}_{all_covid_admissions_table_name}"
# dbutils.widgets.text("all_covid_admissions_table_name", all_covid_admissions_table_name)
all_admission_infection_dates_table_name = f"{preamble}_{all_admission_infection_dates_table_name}"
all_infection_dates_table_name = f"{preamble}_{all_infection_dates_table_name}"
output_table_name = f"{preamble}_{infections_output_table_name}"

print("Joining admission and first tests datasets in order to identify and link first infections as a base cohort...")

# COMMAND ----------

# MAKE AN ARRAY OF ALL DATES ASSOCIATED WITH EACH ADMISSION, THIS ARRAY CAN BE THE ONE THAT YOU ANTI JOIN TESTS WITH
# WANT TO END UP WITH ONE ROW PER INFECTION AMONGST ADMISSIONS, SO ADMIDATE + EPISTART / POSITIVE TEST DATE WITH LABEL SO THAT WE CAN JOIN BACK
# MAYBE DROP THE COALESCING OF COLS OR FIGURE A WAY TO PROPAGATE ACROSS AN ARRAY

# COMMAND ----------

if test:
    all_rows = spark.sql(f"SELECT COUNT(*) FROM {collab_database}.{admissions_table_name}").first()[0]
    distinct_admissions = spark.sql(
        f"SELECT COUNT(DISTINCT PERSON_ID_DEID, ADMIDATE) FROM {collab_database}.{admissions_table_name}"
    ).first()[0]
    distinct_admissions_and_discharges = spark.sql(
        f"SELECT COUNT(DISTINCT PERSON_ID_DEID, ADMIDATE, CASE WHEN DISDATE IS NULL THEN '{study_end}' ELSE DISDATE END) FROM {collab_database}.{admissions_table_name}"
    ).first()[0]
    print(all_rows, distinct_admissions, distinct_admissions_and_discharges)
    assert all_rows == distinct_admissions and distinct_admissions == distinct_admissions_and_discharges

# COMMAND ----------

if test:
    all_rows = spark.sql(f"SELECT COUNT(*) FROM {collab_database}.{tests_table_name}").first()[0]
    distinct_tests = spark.sql(
        f"SELECT COUNT(DISTINCT PERSON_ID_DEID, POSITIVE_TEST_DATE) FROM {collab_database}.{tests_table_name}"
    ).first()[0]
    print(all_rows, distinct_tests)
    assert all_rows == distinct_tests

print(
    f"Determining and linking all positive test-based infections within the case hospitalisation window ({case_hospitalisation_window} days)"
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_admissions_w_tests_in_ch_window AS
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  ALL_POSITIVE_TESTS,
  CASE WHEN SIZE(POSITIVE_TESTS_COVID_EVIDENCE) > 0 THEN 1 ELSE 0 END AS COVID_BY_TEST,
  CASE WHEN SIZE(POSITIVE_TESTS_IN_WINDOW) > 0 THEN 1 ELSE 0 END AS POSITIVE_TEST_IN_WINDOW,
  CASE WHEN SIZE(POSITIVE_TESTS_IN_UKHSA_WINDOW) > 0 THEN 1 ELSE 0 END AS POSITIVE_TEST_IN_UKHSA_WINDOW
FROM (
  SELECT
    a.PERSON_ID_DEID,
    ADMIDATE,
    COLLECT_LIST(POSITIVE_TEST_DATE) AS ALL_POSITIVE_TESTS,
    COLLECT_LIST(CASE WHEN POSITIVE_TEST_DATE >= DATE_SUB(ADMIDATE, 14) THEN POSITIVE_TEST_DATE ELSE NULL END) AS POSITIVE_TESTS_COVID_EVIDENCE,
    COLLECT_LIST(CASE WHEN POSITIVE_TEST_DATE BETWEEN DATE_SUB(ADMIDATE, 14) AND DATE_ADD(ADMIDATE, 6) THEN POSITIVE_TEST_DATE ELSE NULL END) AS POSITIVE_TESTS_IN_WINDOW,
    COLLECT_LIST(CASE WHEN POSITIVE_TEST_DATE >= DATE_SUB(ADMIDATE, 5) THEN POSITIVE_TEST_DATE ELSE NULL END) AS POSITIVE_TESTS_IN_UKHSA_WINDOW
  FROM {collab_database}.{admissions_table_name} a
  LEFT JOIN {collab_database}.{tests_table_name} b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND POSITIVE_TEST_DATE BETWEEN DATE_SUB(ADMIDATE, {case_hospitalisation_window}) AND COALESCE(DISDATE, POSITIVE_TEST_DATE)
  GROUP BY a.PERSON_ID_DEID, ADMIDATE, DISDATE
)"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_admission_infections_from_tests AS
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  'Test' AS INFECTION_SOURCE,
  EXPLODE(ALL_POSITIVE_TESTS) AS INFECTION_DATE
FROM {preamble}_all_admissions_w_tests_in_ch_window"""
)

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT * FROM {preamble}_all_admission_infections_from_tests WHERE PERSON_ID_DEID == '6ORRPEFRFIBS82E'"
        )
    )

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_covid_admissions AS
SELECT *
FROM (
  SELECT
    a.*,
    COVID_BY_TEST,
    ALL_POSITIVE_TESTS,
    POSITIVE_TEST_IN_WINDOW,
    POSITIVE_TEST_IN_UKHSA_WINDOW,
    CASE WHEN a.ADMIDATE == FIRST_VALUE(a.ADMIDATE) OVER (PARTITION BY a.PERSON_ID_DEID ORDER BY a.ADMIDATE ASC) THEN 1 ELSE 0 END AS FIRST_ADMISSION,
    CASE WHEN 
      a.ADMIDATE <= FIRST_VALUE(a.ADMIDATE) OVER (PARTITION BY a.PERSON_ID_DEID ORDER BY a.ADMIDATE ASC)
      AND (a.ADMIDATE <= DATE_ADD(FIRST_POSITIVE_TEST_DATE, {case_hospitalisation_window}) OR FIRST_POSITIVE_TEST_DATE IS NULL)
    THEN 1 ELSE 0 END AS FIRST_INFECTION
  FROM {collab_database}.{admissions_table_name} a
  LEFT JOIN {preamble}_all_admissions_w_tests_in_ch_window b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.ADMIDATE == b.ADMIDATE
  LEFT JOIN (
    SELECT PERSON_ID_DEID, MIN(POSITIVE_TEST_DATE) AS FIRST_POSITIVE_TEST_DATE
    FROM {collab_database}.{tests_table_name}
    GROUP BY PERSON_ID_DEID
  ) c
  ON a.PERSON_ID_DEID == c.PERSON_ID_DEID
  WHERE COVID_BY_CODE == 1 OR PIMS_BY_CODE == 1 OR COVID_BY_TEST == 1
)
-- WHERE COVID_BY_TEST == 1 OR (FIRST_ADMISSION == 1 AND PIMS_BY_CODE == 1) OR (FIRST_ADMISSION == 1 AND COVID_BY_CODE == 1) OR COVID_PRIMARY_BY_CODE == 1 OR PIMS_PRIMARY_BY_CODE == 1
WHERE COVID_BY_TEST == 1 OR (FIRST_ADMISSION == 1 AND PIMS_BY_CODE == 1) OR (FIRST_INFECTION == 1 AND COVID_BY_CODE == 1) OR COVID_PRIMARY_BY_CODE == 1 OR PIMS_PRIMARY_BY_CODE == 1"""
)

# COMMAND ----------

print(f"Creating `{all_covid_admissions_table_name}` with study start date == {study_start}")

spark.sql(f"SELECT * FROM {preamble}_all_covid_admissions").createOrReplaceTempView(all_covid_admissions_table_name)
drop_table(all_covid_admissions_table_name)
create_table(all_covid_admissions_table_name)
optimise_table(all_covid_admissions_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{all_covid_admissions_table_name}")
print(f"`{all_covid_admissions_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT * FROM {collab_database}.{all_covid_admissions_table_name} WHERE PERSON_ID_DEID == '6ORRPEFRFIBS82E'"
        )
    )

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW {preamble}_all_admission_infections_from_diagnosis AS
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  INFECTION_SOURCE,
  INFECTION_DATE
FROM (
  SELECT
    a.PERSON_ID_DEID,
    a.ADMIDATE,
    EXPLODE(MAP("Diagnosis", CASE WHEN FIRST_INFECTION == 1 THEN a.COVID_EPISTART ELSE a.COVID_PRIMARY_EPISTART END, "PIMS-TS", CASE WHEN FIRST_ADMISSION == 1 THEN a.PIMS_EPISTART ELSE a.PIMS_PRIMARY_EPISTART END)) AS (INFECTION_SOURCE, INFECTION_DATE)
--     EXPLODE(MAP("Diagnosis", CASE WHEN FIRST_ADMISSION == 1 THEN a.COVID_EPISTART ELSE a.COVID_PRIMARY_EPISTART END, "PIMS-TS", a.PIMS_EPISTART)) AS (INFECTION_SOURCE, INFECTION_DATE)
  FROM {collab_database}.{admissions_table_name} a
  INNER JOIN {collab_database}.{all_covid_admissions_table_name} b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.ADMIDATE == b.ADMIDATE
) WHERE INFECTION_DATE IS NOT NULL"""
)

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT * FROM {preamble}_all_admission_infections_from_diagnosis WHERE PERSON_ID_DEID == '6ORRPEFRFIBS82E'"
        )
    )

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_admission_infection_dates AS
SELECT
  x.*,
  GREATEST(COALESCE(DISDATE, TO_DATE('{study_end}')), DATE_ADD(INFECTION_DATE, {infection_period_length})) AS INFECTION_END_DATE
FROM (
  SELECT
    PERSON_ID_DEID,
    ADMIDATE,
    CONCAT_WS(" and ", ARRAY_SORT(COLLECT_LIST(INFECTION_SOURCE))) AS INFECTION_SOURCE,
    INFECTION_DATE,
    SIZE(COLLECT_LIST(INFECTION_SOURCE)) AS NUM_SOURCES
  FROM (
    SELECT * FROM {preamble}_all_admission_infections_from_diagnosis
    UNION
    SELECT a.* FROM {preamble}_all_admission_infections_from_tests a INNER JOIN {collab_database}.{all_covid_admissions_table_name} b ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.ADMIDATE == b.ADMIDATE
  )
  GROUP BY PERSON_ID_DEID, ADMIDATE, INFECTION_DATE
) x
INNER JOIN {collab_database}.{admissions_table_name} y
ON x.PERSON_ID_DEID == y.PERSON_ID_DEID AND x.ADMIDATE == y.ADMIDATE"""
)

# COMMAND ----------

print(f"Creating `{all_admission_infection_dates_table_name}` with study start date == {study_start}")

spark.sql(f"SELECT * FROM {preamble}_all_admission_infection_dates").createOrReplaceTempView(
    all_admission_infection_dates_table_name
)
drop_table(all_admission_infection_dates_table_name)
create_table(all_admission_infection_dates_table_name)
optimise_table(all_admission_infection_dates_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{all_admission_infection_dates_table_name}")
print(f"`{all_admission_infection_dates_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT * FROM {collab_database}.{all_admission_infection_dates_table_name} WHERE PERSON_ID_DEID == '6ORRPEFRFIBS82E'"
        )
    )

# COMMAND ----------

if test:
    num_diag_rows = spark.sql(f"SELECT COUNT(*) FROM {preamble}_all_admission_infections_from_diagnosis").first()[0]
    num_test_rows = spark.sql(
        f"SELECT COUNT(*) FROM {preamble}_all_admission_infections_from_tests a INNER JOIN {collab_database}.{all_covid_admissions_table_name} b ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.ADMIDATE == b.ADMIDATE"
    ).first()[0]
    num_post_union = spark.sql(
        f"SELECT SUM(NUM_SOURCES) FROM {collab_database}.{all_admission_infection_dates_table_name}"
    ).first()[0]
    print(num_diag_rows, num_test_rows, num_post_union)
    assert num_diag_rows + num_test_rows == num_post_union

# COMMAND ----------

pool = f"""
  SELECT
    PERSON_ID_DEID,
    ADMIDATE,
    INFECTION_SOURCE,
    INFECTION_DATE,
    INFECTION_END_DATE
  FROM {collab_database}.{all_admission_infection_dates_table_name}"""
if not admissions_only:
    pool += f"""
  UNION
  SELECT
    PERSON_ID_DEID,
    NULL AS ADMIDATE,
    "Test" AS INFECTION_SOURCE,
    POSITIVE_TEST_DATE AS INFECTION_DATE,
    DATE_ADD(POSITIVE_TEST_DATE, {infection_period_length}) AS INFECTION_END_DATE
  FROM (
    SELECT a.*
    FROM {collab_database}.{tests_table_name} a
    LEFT ANTI JOIN {collab_database}.{all_admission_infection_dates_table_name} b
    ON a.PERSON_ID_DEID == b.PERSON_ID_DEID and a.POSITIVE_TEST_DATE == b.INFECTION_DATE
    WHERE POSITIVE_TEST_DATE IS NOT NULL
  )"""

# COMMAND ----------

all_infections = spark.sql(
    f"""
SELECT * FROM ({pool}
)
WHERE INFECTION_DATE <= '{infection_censoring_date}'"""
)

print(f"Creating `{all_infection_dates_table_name}` with study start date == {study_start}")

all_infections.createOrReplaceTempView(all_infection_dates_table_name)
drop_table(all_infection_dates_table_name)
create_table(all_infection_dates_table_name)
optimise_table(all_infection_dates_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{all_infection_dates_table_name}")
print(f"`{all_infection_dates_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

# Select all columns from both tables and coalesce the ones that are in both, preferring records from admissions, as DOB is higher resolution here
admissions_col_names = spark.sql(f"SELECT * FROM {collab_database}.{all_covid_admissions_table_name}").schema.names
tests_col_names = spark.sql(f"SELECT * FROM {collab_database}.{tests_table_name}").schema.names

cols_to_include = ",\n      ".join(
    [
        f"COALESCE(b.{col_name}, c.{col_name}) AS {col_name}"
        if col_name in tests_col_names and col_name in admissions_col_names
        else f"`{col_name}`"
        for col_name in unique_and_filter(
            admissions_col_names + tests_col_names,
            [
                "PERSON_ID_DEID",
                "ADMIDATE",
                "POSITIVE_TEST_DATE",
                "ADMISSION_VARIANT_PERIOD",
                "POSITIVE_TEST_VARIANT_PERIOD",
                "TYPE_OF_ADMISSION",
            ],
        )
    ]
)

if test:
    print(admissions_col_names)
    print(tests_col_names)
    print(cols_to_include)

# COMMAND ----------


def generate_nth_table_name(iteration):
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(iteration % 10 if iteration % 100 not in [11, 12, 13] else 0, "th")
    return f"{preamble}_{iteration}{suffix}_infections"


# COMMAND ----------


def create_partial_cohort(iteration):
    return spark.sql(
        f"""
SELECT *
FROM (
  SELECT
    GREATEST(0, DATEDIFF(INFECTION_DATE, DATE_OF_BIRTH) / 365.25) AS AGE,
    DATEDIFF('{study_start}', DATE_OF_BIRTH) / 365.25 AS AGE_AT_STUDY_START,
    CASE WHEN ADMIDATE IS NOT NULL THEN 1 ELSE 0 END AS ADMISSION_IN_WINDOW,
    {generate_variant_period_case_when('INFECTION_DATE', variant_period_dict)} AS INFECTION_VARIANT_PERIOD,
    *
  FROM (
    SELECT
      a.PERSON_ID_DEID,
      a.ADMIDATE,
      INFECTION_SOURCE,
      INFECTION_DATE,
      INFECTION_END_DATE,
      CASE WHEN INFECTION_DATE >= DATE_ADD(a.ADMIDATE, 7) THEN 'Nosocomial' ELSE TYPE_OF_ADMISSION END AS TYPE_OF_ADMISSION,
      COALESCE(a.ADMIDATE, INFECTION_DATE) AS LOOKBACK_DATE,
      {cols_to_include}
    FROM (
      SELECT * FROM (
        -- NEED THE ORDER BY ADMIDATE AS THERE MIGHT BE THE SAME INFECTION DATE FOR TWO DIFFERENT ADMISSIONS IN THE LIST ETC, we always want to take the earliest event (potentially up for debate?)
        SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x.PERSON_ID_DEID ORDER BY x.INFECTION_DATE ASC, x.ADMIDATE ASC) RN
        FROM {collab_database}.{all_infection_dates_table_name} x
        {'INNER JOIN (SELECT PERSON_ID_DEID, INFECTION_END_DATE FROM ' + collab_database + '.' + generate_nth_table_name(iteration - 1) + ') y ON x.PERSON_ID_DEID == y.PERSON_ID_DEID AND x.INFECTION_DATE > y.INFECTION_END_DATE' if iteration > 1 else ''}
      ) WHERE RN == 1
    ) a
    LEFT JOIN {collab_database}.{all_covid_admissions_table_name} b
    ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.ADMIDATE = b.ADMIDATE
    LEFT JOIN {collab_database}.{tests_table_name} c
    ON a.PERSON_ID_DEID == c.PERSON_ID_DEID AND INFECTION_DATE = c.POSITIVE_TEST_DATE
  )
) WHERE AGE < 18 AND INFECTION_DATE <= '{infection_censoring_date}'"""
    )


# COMMAND ----------


def create_cohort(n_infections=1):
    iteration = 1
    table_count = 1
    partial_table_statements = []
    while table_count > 0 and iteration <= n_infections:
        partial_table_name = generate_nth_table_name(iteration)
        table = create_partial_cohort(iteration)
        table.createOrReplaceTempView(partial_table_name)
        drop_table(partial_table_name)
        create_table(partial_table_name)
        optimise_table(partial_table_name, "PERSON_ID_DEID")

        table = spark.sql(f"SELECT * FROM {collab_database}.{partial_table_name}")
        table_count = table.count()
        print(f"`{partial_table_name}` has {table_count} rows and {len(table.columns)} columns.")
        partial_table_statements.append(
            f"SELECT *, {iteration} AS INFECTION_IDX FROM {collab_database}.{partial_table_name}"
        )
        iteration += 1
    return spark.sql(
        f"SELECT * FROM ({' UNION '.join(partial_table_statements)}) WHERE INFECTION_DATE >= '{study_start}'" ""
    )


# COMMAND ----------

if include_reinfections:
    n_infections = 100
else:
    n_infections = 1

full_cohort = create_cohort(n_infections=n_infections)

# COMMAND ----------

print(f"Creating `{output_table_name}` with study start date == {study_start}")

full_cohort.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

display(
    spark.sql(
        f"SELECT COUNT(*) AS COVID_ADMISSIONS_IN_COHORT FROM {collab_database}.{output_table_name} WHERE ADMISSION_IN_WINDOW == 1"
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"SELECT INFECTION_IDX, COUNT(*) AS COVID_ADMISSIONS_IN_COHORT, SUM(CASE WHEN TYPE_OF_ADMISSION == 'PIMS' THEN 1 ELSE 0 END) AS NUM_PIMS FROM {collab_database}.{output_table_name} WHERE ADMISSION_IN_WINDOW == 1 GROUP BY INFECTION_IDX ORDER BY INFECTION_IDX ASC"
    )
)

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT * FROM (SELECT COUNT(*) OVER (PARTITION BY PERSON_ID_DEID, LOOKBACK_DATE) AS NR, * FROM {collab_database}.{output_table_name}) WHERE NR > 1"
        )
    )

# COMMAND ----------

if test:
    display(spark.sql(f"SELECT * FROM {collab_database}.{output_table_name} WHERE PERSON_ID_DEID == 'HHK1BMISY7EVVNX'"))

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT SUM(CASE WHEN INFECTION_DATE > DISDATE THEN 1 ELSE 0 END) AS DISDATE_ERRORS FROM {collab_database}.{output_table_name}"
        )
    )

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT SUM(CASE WHEN ICU == 1 THEN 1 ELSE 0 END) AS NUM_ICU, SUM(CASE WHEN DAY_CASE == 1 THEN 1 ELSE 0 END) AS NUM_DAY_CASE, SUM(CASE WHEN STILL_IN_HOSPITAL == 1 THEN 1 ELSE 0 END) AS NUM_STILL_IN_HOSPITAL FROM {collab_database}.{output_table_name}"
        )
    )

# COMMAND ----------

if test:
    display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT PERSON_ID_DEID) FROM {collab_database}.{output_table_name}"))

# COMMAND ----------

if test:
    display(
        spark.sql(
            f"SELECT TYPE_OF_ADMISSION, COUNT(*) FROM {collab_database}.{output_table_name} WHERE ADMISSION_IN_WINDOW == 1 GROUP BY TYPE_OF_ADMISSION"
        )
    )

# COMMAND ----------

if test:
    display(spark.sql(f"SELECT * FROM {collab_database}.{output_table_name} WHERE TYPE_OF_ADMISSION == 'Nosocomial'"))

# COMMAND ----------

if test:
    unknown_primary_code_counts = spark.sql(
        f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(ARRAY_DISTINCT(SPLIT(DIAG_4_CONCAT_PRIMARY, ','))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE TYPE_OF_ADMISSION == 'Unknown'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC"""
    )

    table_name = f"{preamble}_unknown_primary_code_counts"

    print(f"Creating `{table_name}` with study start date == {study_start}")

    unknown_primary_code_counts.createOrReplaceTempView(table_name)
    drop_table(table_name)
    create_table(table_name)

    table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
    print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")

    display(table)

# COMMAND ----------

if test:
    unknown_secondary_code_counts = spark.sql(
        f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(ARRAY_DISTINCT(SPLIT(DIAG_4_CONCAT_SECONDARY, ','))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE TYPE_OF_ADMISSION == 'Unknown'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC"""
    )

    table_name = f"{preamble}_unknown_secondary_code_counts"

    print(f"Creating `{table_name}` with study start date == {study_start}")

    unknown_secondary_code_counts.createOrReplaceTempView(table_name)
    drop_table(table_name)
    create_table(table_name)

    table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
    print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")

    display(table)

# COMMAND ----------

if test:
    pims_primary_code_counts = spark.sql(
        f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(ARRAY_DISTINCT(SPLIT(DIAG_4_CONCAT_PRIMARY, ','))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE TYPE_OF_ADMISSION == 'PIMS'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC"""
    )

    table_name = f"{preamble}_pims_primary_code_counts"

    print(f"Creating `{table_name}` with study start date == {study_start}")

    pims_primary_code_counts.createOrReplaceTempView(table_name)
    drop_table(table_name)
    create_table(table_name)

    table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
    print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")

    display(table)

# COMMAND ----------

if test:
    pims_secondary_code_counts = spark.sql(
        f"""
SELECT COUNT(*) AS N_OCCURRENCES, ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION FROM (
  SELECT EXPLODE(ARRAY_DISTINCT(SPLIT(DIAG_4_CONCAT_SECONDARY, ','))) AS ICD10_CODE
  FROM {collab_database}.{output_table_name}
  WHERE TYPE_OF_ADMISSION == 'PIMS'
)
INNER JOIN dss_corporate.icd10_group_chapter_v01
ON ICD10_CODE = ALT_CODE
GROUP BY ICD10_CODE, ICD10_DESCRIPTION, DESCRIPTIONS_ABBREVIATED, ICD10_CHAPTER_HEADING, ICD10_CHAPTER_DESCRIPTION, ICD10_GROUP_HEADING, ICD10_GROUP_DESCRIPTION
ORDER BY N_OCCURRENCES DESC"""
    )

    table_name = f"{preamble}_pims_secondary_code_counts"

    print(f"Creating `{table_name}` with study start date == {study_start}")

    pims_secondary_code_counts.createOrReplaceTempView(table_name)
    drop_table(table_name)
    create_table(table_name)

    table = spark.sql(f"SELECT * FROM {collab_database}.{table_name} ORDER BY N_OCCURRENCES DESC")
    print(f"`{table_name}` has {table.count()} rows and {len(table.columns)} columns.")

    display(table)
