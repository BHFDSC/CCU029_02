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

input_table_name = f"{preamble}_{cohort_w_lookback_output_table_name}"
cohort_codelist_weight_table_name = f"{preamble}_{cohort_codelist_weight_table_name}"
cohort_codelist_height_table_name = f"{preamble}_{cohort_codelist_height_table_name}"
cohort_all_weights_table_name = f"{preamble}_{cohort_all_weights_table_name}"
cohort_all_heights_table_name = f"{preamble}_{cohort_all_heights_table_name}"
cohort_best_weight_and_height_table_name = f"{preamble}_{cohort_best_weight_and_height_table_name}"
output_table_name = f"{preamble}_{cohort_w_bmi_output_table_name}"


print("Subsetting GDPPR to only the records that fall within each person in the cohort's admission date and 2 years prior to it...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_gdppr_subset AS
SELECT b.LOOKBACK_DATE, a.*
FROM {collab_database}.{preamble}_gdppr a
INNER JOIN {collab_database}.{input_table_name} b
ON a.NHS_NUMBER_DEID = b.PERSON_ID_DEID AND a.DATE BETWEEN ADD_MONTHS(b.LOOKBACK_DATE, -24) AND b.LOOKBACK_DATE""")

# COMMAND ----------

# Lists are from OpenSAFELY, we will use these to identify weight and height data from the GDPPR subset defined above
# 
# https://www.opencodelists.org/codelist/opensafely/weight-snomed/5459abc6/#full-list
# 
# https://www.opencodelists.org/codelist/opensafely/height-snomed/3b4a3891/#full-list

# COMMAND ----------

import io
import pandas as pd

print(f"Generating `{cohort_codelist_weight_table_name}` and `{cohort_codelist_height_table_name}` to identify weight and height records in GDPPR...")

weight_snomed = """
code,term
139985004,O/E - weight
162763007,On examination - weight
248341004,General weight finding
248345008,Body weight
27113001,Body weight
271604008,Weight finding
301333006,Finding of measures of body weight
363808001,Measured body weight
424927000,Body weight with shoes
425024002,Body weight without shoes
735395000,Current body weight
784399000,Self reported body weight
"""

df = pd.read_csv(io.StringIO(weight_snomed))
df = spark.createDataFrame(df)
df.createOrReplaceTempView(cohort_codelist_weight_table_name)
drop_table(cohort_codelist_weight_table_name)
create_table(cohort_codelist_weight_table_name)

height_snomed = """
code,term
139977008,O/E - height
14456009,Measuring height of patient
162755006,On examination - height
248327008,General finding of height
248333004,Standing height
50373000,Body height measure
"""

df = pd.read_csv(io.StringIO(height_snomed))
df = spark.createDataFrame(df)
df.createOrReplaceTempView(cohort_codelist_height_table_name)
drop_table(cohort_codelist_height_table_name)
create_table(cohort_codelist_height_table_name)

# COMMAND ----------

print("Creating weight and height tables for the cohort...")

# COMMAND ----------

# Apply transformations to weights and heights data to try and standardise units, cut off erroneous records etc.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_weights AS
SELECT
  NHS_NUMBER_DEID,
  LOOKBACK_DATE,
  DATE AS WEIGHT_DATE,
  VALUE1_CONDITION AS WEIGHT_DIRTY,
  CASE WHEN VALUE1_CONDITION > 400 THEN VALUE1_CONDITION / 1000 ELSE VALUE1_CONDITION END AS WEIGHT,
  CASE WHEN (CASE WHEN VALUE1_CONDITION > 400 THEN VALUE1_CONDITION / 1000 ELSE VALUE1_CONDITION END) BETWEEN 0.2 AND 400 THEN 1 ELSE 0 END AS ACCEPTABLE_WEIGHT,
  gp.CODE AS WEIGHT_CODE
FROM {preamble}_gdppr_subset gp
INNER JOIN {collab_database}.{cohort_codelist_weight_table_name} lkp_wt
ON gp.CODE = lkp_wt.code
WHERE VALUE1_CONDITION IS NOT NULL""")

# COMMAND ----------

cohort_weights = spark.sql(f"SELECT * FROM {preamble}_all_weights WHERE ACCEPTABLE_WEIGHT == 1")

print(f"Creating `{cohort_all_weights_table_name}` with study start date == {study_start}")

cohort_weights.createOrReplaceTempView(cohort_all_weights_table_name)
drop_table(cohort_all_weights_table_name)
create_table(cohort_all_weights_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_all_weights_table_name}")
print(f"`{cohort_all_weights_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_heights AS
SELECT
  NHS_NUMBER_DEID,
  LOOKBACK_DATE,
  DATE AS HEIGHT_DATE,
  VALUE1_CONDITION AS HEIGHT_DIRTY,
  CASE WHEN VALUE1_CONDITION > 2.5 THEN VALUE1_CONDITION / 100 ELSE VALUE1_CONDITION END AS HEIGHT,
  CASE WHEN (CASE WHEN VALUE1_CONDITION > 2.5 THEN VALUE1_CONDITION / 100 ELSE VALUE1_CONDITION END) BETWEEN 0.25 AND 2.5 THEN 1 ELSE 0 END AS ACCEPTABLE_HEIGHT,
  gp.CODE AS HEIGHT_CODE
FROM {preamble}_gdppr_subset gp
INNER JOIN {collab_database}.{cohort_codelist_height_table_name} lkp_ht
ON gp.CODE = lkp_ht.code
WHERE VALUE1_CONDITION IS NOT NULL""")

# COMMAND ----------

cohort_heights = spark.sql(f"SELECT * FROM {preamble}_all_heights WHERE ACCEPTABLE_HEIGHT == 1")

print(f"Creating `{cohort_all_heights_table_name}` with study start date == {study_start}")

cohort_heights.createOrReplaceTempView(cohort_all_heights_table_name)
drop_table(cohort_all_heights_table_name)
create_table(cohort_all_heights_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_all_heights_table_name}")
print(f"`{cohort_all_heights_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

print("Finding minimum time period between records for individuals with both height and weight data; calculating BMI using this data...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_all_weights_and_heights AS
SELECT
  COALESCE(a.NHS_NUMBER_DEID, b.NHS_NUMBER_DEID) AS NHS_NUMBER_DEID,
  COALESCE(a.LOOKBACK_DATE, b.LOOKBACK_DATE) AS LOOKBACK_DATE,
  ABS(DATEDIFF(WEIGHT_DATE, HEIGHT_DATE)) AS WEIGHT_HEIGHT_DATE_DELTA,
  WEIGHT_DATE,
  WEIGHT,
  WEIGHT_CODE,
  HEIGHT_DATE,
  HEIGHT,
  HEIGHT_CODE
FROM {collab_database}.{cohort_all_weights_table_name} a
FULL OUTER JOIN {collab_database}.{cohort_all_heights_table_name} b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_min_weight_height_deltas AS
SELECT
  NHS_NUMBER_DEID,
  LOOKBACK_DATE,
  MIN(WEIGHT_HEIGHT_DATE_DELTA) AS MIN_WEIGHT_HEIGHT_DATE_DELTA
FROM {preamble}_all_weights_and_heights
GROUP BY NHS_NUMBER_DEID, LOOKBACK_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_max_weight_height_dates AS
SELECT
  a.NHS_NUMBER_DEID,
  a.LOOKBACK_DATE,
  CASE WHEN MAX(WEIGHT_DATE) >= MAX(HEIGHT_DATE) THEN MAX(WEIGHT_DATE) ELSE MAX(HEIGHT_DATE) END AS MAX_DATE,
  CASE WHEN MAX(WEIGHT_DATE) >= MAX(HEIGHT_DATE) THEN "WEIGHT" ELSE "HEIGHT" END AS MAX_DATE_TYPE
FROM {preamble}_all_weights_and_heights a
INNER JOIN {preamble}_min_weight_height_deltas b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE AND a.WEIGHT_HEIGHT_DATE_DELTA = b.MIN_WEIGHT_HEIGHT_DATE_DELTA
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_best_weight_and_height AS
SELECT
  a.NHS_NUMBER_DEID,
  a.LOOKBACK_DATE,
  a.WEIGHT_HEIGHT_DATE_DELTA,
  AVG(WEIGHT) / (AVG(HEIGHT) * AVG(HEIGHT)) AS BMI,
  a.WEIGHT_DATE,
  a.HEIGHT_DATE,
  AVG(WEIGHT) AS WEIGHT,
  AVG(HEIGHT) AS HEIGHT
FROM {preamble}_all_weights_and_heights a
INNER JOIN {preamble}_min_weight_height_deltas b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE AND a.WEIGHT_HEIGHT_DATE_DELTA = b.MIN_WEIGHT_HEIGHT_DATE_DELTA
INNER JOIN {preamble}_max_weight_height_dates c
ON a.NHS_NUMBER_DEID = c.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = c.LOOKBACK_DATE AND ((MAX_DATE_TYPE = "WEIGHT" AND WEIGHT_DATE = MAX_DATE) OR (MAX_DATE_TYPE = "HEIGHT" AND HEIGHT_DATE = MAX_DATE))
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE, a.WEIGHT_HEIGHT_DATE_DELTA, a.WEIGHT_DATE, a.HEIGHT_DATE""")

# COMMAND ----------

cohort_w_weight_and_height = spark.sql(f"SELECT * FROM {preamble}_best_weight_and_height")

print(f"Creating `{cohort_best_weight_and_height_table_name}` with study start date == {study_start}")

cohort_w_weight_and_height.createOrReplaceTempView(cohort_best_weight_and_height_table_name)
drop_table(cohort_best_weight_and_height_table_name)
create_table(cohort_best_weight_and_height_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_best_weight_and_height_table_name}")
print(f"`{cohort_best_weight_and_height_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

print("For those indivudals without both weight and height, finding the most recent weight / height...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_best_weight_max_dates AS
SELECT a.NHS_NUMBER_DEID, a.LOOKBACK_DATE, MAX(a.WEIGHT_DATE) AS MAX_WEIGHT_DATE
FROM {collab_database}.{cohort_all_weights_table_name} a
-- ANTI JOIN {collab_database}.{preamble}_wip_hosp_cohort_best_weight_and_height b
ANTI JOIN {collab_database}.{cohort_best_weight_and_height_table_name} b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_best_height_max_dates AS
SELECT a.NHS_NUMBER_DEID, a.LOOKBACK_DATE, MAX(a.HEIGHT_DATE) AS MAX_HEIGHT_DATE
FROM {collab_database}.{cohort_all_heights_table_name} a
-- ANTI JOIN {collab_database}.{preamble}_wip_hosp_cohort_best_weight_and_height b
ANTI JOIN {collab_database}.{cohort_best_weight_and_height_table_name} b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_best_weight AS
SELECT
  a.NHS_NUMBER_DEID,
  a.LOOKBACK_DATE,
  a.WEIGHT_DATE,
  AVG(WEIGHT) AS WEIGHT
FROM {collab_database}.{cohort_all_weights_table_name} a
INNER JOIN {preamble}_best_weight_max_dates b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE AND a.WEIGHT_DATE = b.MAX_WEIGHT_DATE
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE, a.WEIGHT_DATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_best_height AS
SELECT
  a.NHS_NUMBER_DEID,
  a.LOOKBACK_DATE,
  a.HEIGHT_DATE,
  AVG(HEIGHT) AS HEIGHT
FROM {collab_database}.{cohort_all_heights_table_name} a
INNER JOIN {preamble}_best_height_max_dates b
ON a.NHS_NUMBER_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE AND a.HEIGHT_DATE = b.MAX_HEIGHT_DATE
GROUP BY a.NHS_NUMBER_DEID, a.LOOKBACK_DATE, a.HEIGHT_DATE""")

# COMMAND ----------

spark.sql(f"SELECT * FROM {collab_database}.{cohort_best_weight_and_height_table_name}") \
  .unionByName(spark.sql(f"SELECT *, NULL AS WEIGHT_HEIGHT_DATE_DELTA, NULL AS BMI, NULL AS HEIGHT, NULL AS HEIGHT_DATE FROM {preamble}_best_weight")) \
  .unionByName(spark.sql(f"SELECT *, NULL AS WEIGHT_HEIGHT_DATE_DELTA, NULL AS BMI, NULL AS WEIGHT, NULL AS WEIGHT_DATE FROM {preamble}_best_height")) \
  .orderBy("NHS_NUMBER_DEID") \
  .createOrReplaceTempView(f"{preamble}_best_weight_and_height_to_join")

print("Joining the union of the weight, height and BMI info to the cohort...")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT COUNT(*), COUNT(DISTINCT NHS_NUMBER_DEID) FROM {preamble}_best_weight_and_height_to_join"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT * FROM {collab_database}.{cohort_best_weight_and_height_table_name} ORDER BY BMI DESC"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_w_bmi AS
SELECT
  a.*,
  WEIGHT_HEIGHT_DATE_DELTA,
  BMI,
  WEIGHT_DATE,
  HEIGHT_DATE,
  WEIGHT,
  HEIGHT,
  CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END AS HAS_BMI,
  CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END AS HAS_HEIGHT,
  CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END AS HAS_WEIGHT
FROM {collab_database}.{input_table_name} a
LEFT JOIN {preamble}_best_weight_and_height_to_join b
ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE""")

# COMMAND ----------

cohort_w_weights = spark.sql(f"SELECT * FROM {preamble}_w_bmi")

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_weights.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT ADMISSION_IN_WINDOW, COUNT(*), SUM(CASE WHEN WEIGHT IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN HEIGHT IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN BMI IS NOT NULL THEN 1 ELSE 0 END), SUM(CASE WHEN BMI IS NOT NULL AND WEIGHT IS NOT NULL AND HEIGHT IS NOT NULL THEN 1 ELSE 0 END) FROM {collab_database}.{preamble}_wip_cohort_w_bmi GROUP BY ADMISSION_IN_WINDOW"))

# COMMAND ----------

if test:
  display(spark.sql(f"SELECT PERSON_ID_DEID, BMI, WEIGHT, HEIGHT, * FROM (SELECT *, FIRST_VALUE(INFECTION_IDX) OVER (PARTITION BY PERSON_ID_DEID ORDER BY INFECTION_IDX DESC) AS MAX_INFECTION_IDX FROM {collab_database}.{preamble}_wip_cohort_w_bmi) WHERE MAX_INFECTION_IDX > 3"))
