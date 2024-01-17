# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../02/config/quiet $skip=skip

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

input_table_name = f"{preamble}_{infections_output_table_name}"
# dbutils.widgets.text("input_table_name", input_table_name)
cohort_hes_apc_lookback_table_name = f"{preamble}_{cohort_hes_apc_lookback_table_name}"
cohort_hes_op_lookback_table_name = f"{preamble}_{cohort_hes_op_lookback_table_name}"
cohort_gdppr_lookback_table_name = f"{preamble}_{cohort_gdppr_lookback_table_name}"
output_table_name = f"{preamble}_{cohort_w_lookback_output_table_name}"

print(f"Subsetting each of the three lookback datasets to just the IDs included in the cohort (`{input_table_name}`)...")

# COMMAND ----------

# Subset each of the three lookback datasets to just the IDs included in the cohort

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_subset AS
SELECT a.*
FROM {collab_database}.{preamble}_hes_apc a
INNER JOIN {collab_database}.{input_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_op_subset AS
SELECT a.*
FROM {collab_database}.{preamble}_hes_op a
INNER JOIN {collab_database}.{input_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_gdppr_subset AS
SELECT a.*
FROM {collab_database}.{preamble}_gdppr a
INNER JOIN {collab_database}.{input_table_name} b
ON a.NHS_NUMBER_DEID = b.PERSON_ID_DEID""")

# COMMAND ----------

def generate_lookback_table_statement(hes_type, periods):
  tables = [f"{preamble}_{hes_type}_{period}_lookback" for period in periods]
  col_names = []
  joins = []
  for i, table in enumerate(tables):
    col_names += [col_name for col_name in spark.sql(f"SELECT * FROM {table}").schema.names if col_name not in ["PERSON_ID_DEID", "LOOKBACK_DATE"]]
    joins.append(f"LEFT JOIN {table} {chr(98 + i)}\nON a.PERSON_ID_DEID = {chr(98 + i)}.PERSON_ID_DEID AND a.LOOKBACK_DATE = {chr(98 + i)}.LOOKBACK_DATE\n")
  col_names_formatted = ",\n  ".join(col_names)
  sql_statement = f"""
SELECT
  a.PERSON_ID_DEID,
  a.LOOKBACK_DATE,
  {col_names_formatted}
FROM {collab_database}.{input_table_name} a
{''.join(joins)}"""
  return sql_statement, col_names

# COMMAND ----------

print("Creating consistent lookback tables for HES APC...")

# COMMAND ----------

# Similar to our cohort creation, we are initially concerned with consistency.
# 
# We ensure each admission-person pair has a consistent discharge date, then filter to only valid records i.e. non-NULL admission date and a valid discharge date occurring on or after admission.
# 
# We then group by admission date and concatenate all of the codes we are interested in to then be able to lookback through relative to each admission in our cohort.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_fixed_disdate AS
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  MAX(DISDATE) AS DISDATE_FIXED
FROM {preamble}_hes_apc_subset
GROUP BY PERSON_ID_DEID, ADMIDATE""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_consistent AS
SELECT
  a.PERSON_ID_DEID,
  a.ADMIDATE,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(a.DIAG_4_CONCAT, ','))))), '') AS DAILY_HES_APC_ICD10_CODES,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(a.OPERTN_3_CONCAT, '-'), ','))))), '') AS DAILY_OPERTN_3_CONCAT,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(a.OPERTN_4_CONCAT, '-'), ','))))), '') AS DAILY_OPERTN_4_CONCAT
FROM {preamble}_hes_apc_subset a
INNER JOIN {preamble}_hes_apc_fixed_disdate b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.ADMIDATE = b.ADMIDATE
WHERE
  a.PERSON_ID_DEID IS NOT NULL
  AND a.ADMIDATE IS NOT NULL
  AND (DISDATE_FIXED >= a.ADMIDATE OR DISDATE_FIXED IS NULL)
GROUP BY a.PERSON_ID_DEID, a.ADMIDATE""")

# COMMAND ----------

def generate_hes_apc_lookback_period_table_query(period, num_months):
  return f"""CREATE OR REPLACE TEMP VIEW {preamble}_hes_apc_{period}_lookback AS
SELECT
  a.PERSON_ID_DEID,
  a.LOOKBACK_DATE,
  COUNT(*) AS {period.upper()}_HES_APC_COUNT,
  NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.ADMIDATE, ''))), '') AS {period.upper()}_HES_APC_DATES,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_APC_ICD10_CODES, ','))))) AS {period.upper()}_HES_APC_DIAG,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_3_CONCAT, '-'), ','))))) AS {period.upper()}_HES_APC_OPERTN_3_CONCAT,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(NULLIF(b.DAILY_OPERTN_4_CONCAT, '-'), ','))))) AS {period.upper()}_HES_APC_OPERTN_4_CONCAT
FROM {collab_database}.{input_table_name} a
INNER JOIN (SELECT * FROM {preamble}_hes_apc_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, ADMIDATE ASC) b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.ADMIDATE {'<= a.LOOKBACK_DATE' if period == 'apa' else 'BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -' + str(num_months) + ') AND a.LOOKBACK_DATE'}
GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE"""

for period, num_months in lookback_periods.items():
  spark.sql(generate_hes_apc_lookback_period_table_query(period, num_months))

# COMMAND ----------

hes_apc_statement, hes_apc_cols = generate_lookback_table_statement("hes_apc", lookback_periods.keys())

hes_apc_lookback = spark.sql(hes_apc_statement)

print(f"Creating `{cohort_hes_apc_lookback_table_name}` with study start date == {study_start}")

hes_apc_lookback.createOrReplaceTempView(cohort_hes_apc_lookback_table_name)
drop_table(cohort_hes_apc_lookback_table_name)
create_table(cohort_hes_apc_lookback_table_name)
optimise_table(cohort_hes_apc_lookback_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_hes_apc_lookback_table_name}")
print(f"`{cohort_hes_apc_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

print("Creating consistent lookback tables for HES OP...")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_hes_op_consistent AS
SELECT
  a.PERSON_ID_DEID,
  a.APPTDATE,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(a.DIAG_4_CONCAT, ','))))), '') AS DAILY_HES_OP_ICD10_CODES
FROM {preamble}_hes_op_subset a
INNER JOIN {preamble}_hes_op_subset b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID
WHERE
  a.PERSON_ID_DEID IS NOT NULL
  AND a.APPTDATE IS NOT NULL
GROUP BY a.PERSON_ID_DEID, a.APPTDATE""")

# COMMAND ----------

def generate_hes_op_lookback_period_table_query(period, num_months):
  return f"""CREATE OR REPLACE TEMP VIEW {preamble}_hes_op_{period}_lookback AS
SELECT
  a.PERSON_ID_DEID,
  a.LOOKBACK_DATE,
  COUNT(*) AS {period.upper()}_HES_OP_COUNT,
  NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.APPTDATE, ''))), '') AS {period.upper()}_HES_OP_DATES,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_HES_OP_ICD10_CODES, ','))))) AS {period.upper()}_HES_OP_DIAG
FROM {collab_database}.{input_table_name} a
INNER JOIN (SELECT * FROM {preamble}_hes_op_consistent DISTRIBUTE BY PERSON_ID_DEID SORT BY PERSON_ID_DEID, APPTDATE ASC) b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND b.APPTDATE {'<= a.LOOKBACK_DATE' if period == 'apa' else 'BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -' + str(num_months) + ') AND a.LOOKBACK_DATE'}
GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE"""

for period, num_months in lookback_periods.items():
  spark.sql(generate_hes_op_lookback_period_table_query(period, num_months))

# COMMAND ----------

hes_op_statement, hes_op_cols = generate_lookback_table_statement("hes_op", lookback_periods.keys())

hes_op_lookback = spark.sql(hes_op_statement)

print(f"Creating `{cohort_hes_op_lookback_table_name}` with study start date == {study_start}")

hes_op_lookback.createOrReplaceTempView(cohort_hes_op_lookback_table_name)
drop_table(cohort_hes_op_lookback_table_name)
create_table(cohort_hes_op_lookback_table_name)
optimise_table(cohort_hes_op_lookback_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_hes_op_lookback_table_name}")
print(f"`{cohort_hes_op_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

print("Creating consistent lookback tables for GDPPR...")

# COMMAND ----------

# GDPPR is a special case as it is coded under SNOMED rather than ICD-10, so we introduce an intermediary step that converts the SNOMED codes to ICD-10 via a TRUD mapping

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW {preamble}_gdppr_consistent AS
SELECT
  gp.NHS_NUMBER_DEID,
  gp.DATE,
  NULLIF(CONCAT_WS(',', ARRAY_DISTINCT(COLLECT_LIST(trud.ICD))), '') AS DAILY_GDPPR_ICD10_CODES
FROM {preamble}_gdppr_subset gp
INNER JOIN (
  SELECT
    REFERENCED_COMPONENT_ID, -- SNOMED-CT code
    MAP_TARGET as ICD
  FROM dss_corporate.snomed_ct_rf2_map_to_icd10_v01
  WHERE substring(MAP_TARGET,1,1) != "#" AND ICD10_VERSION = "5th Ed"
) trud
ON gp.CODE = trud.REFERENCED_COMPONENT_ID
WHERE
  gp.NHS_NUMBER_DEID IS NOT NULL
  AND gp.DATE IS NOT NULL
GROUP BY gp.NHS_NUMBER_DEID, gp.DATE""")

# COMMAND ----------

def generate_gdppr_lookback_period_table_query(period, num_months):
  return f"""CREATE OR REPLACE TEMP VIEW {preamble}_gdppr_{period}_lookback AS
SELECT
  a.PERSON_ID_DEID,
  a.LOOKBACK_DATE,
  COUNT(*) AS {period.upper()}_GDPPR_COUNT,
  NULLIF(CONCAT_WS(',', COLLECT_LIST(COALESCE(b.DATE, ''))), '') AS {period.upper()}_GDPPR_DATES,
  CONCAT_WS(',', ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(b.DAILY_GDPPR_ICD10_CODES, ','))))) AS {period.upper()}_GDPPR_DIAG
FROM {collab_database}.{input_table_name} a
INNER JOIN (SELECT * FROM {preamble}_gdppr_consistent DISTRIBUTE BY NHS_NUMBER_DEID SORT BY NHS_NUMBER_DEID, DATE ASC) b
ON a.PERSON_ID_DEID = b.NHS_NUMBER_DEID AND b.DATE {'<= a.LOOKBACK_DATE' if period == 'apa' else 'BETWEEN ADD_MONTHS(a.LOOKBACK_DATE, -' + str(num_months) + ') AND a.LOOKBACK_DATE'}
GROUP BY a.PERSON_ID_DEID, a.LOOKBACK_DATE"""

for period, num_months in lookback_periods.items():
  spark.sql(generate_gdppr_lookback_period_table_query(period, num_months))

# COMMAND ----------

gdppr_statement, gdppr_cols = generate_lookback_table_statement("gdppr", lookback_periods.keys())

gdppr_lookback = spark.sql(gdppr_statement)

print(f"Creating `{cohort_gdppr_lookback_table_name}` with study start date == {study_start}")

gdppr_lookback.createOrReplaceTempView(cohort_gdppr_lookback_table_name)
drop_table(cohort_gdppr_lookback_table_name)
create_table(cohort_gdppr_lookback_table_name)
optimise_table(cohort_gdppr_lookback_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{cohort_gdppr_lookback_table_name}")
print(f"`{cohort_gdppr_lookback_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

print("Joining all of the lookback tables to the main cohort...")

# COMMAND ----------

def create_concatenated_cols(periods):
  periods = [period.upper() for period in periods]
  cols = []
  for period in periods:
    cols.append(" + ".join([f"COALESCE({period}_{source}_COUNT, 0)" for source in ["HES_APC", "HES_OP", "GDPPR"]]) + f" AS {period}_COUNT")
    cols.append(" + ".join([f"COALESCE({period}_{source}_COUNT, 0)" for source in ["HES_APC", "HES_OP"]]) + f" AS {period}_COUNT_NO_GDPPR")
    cols.append("CONCAT_WS(',', " + ", ".join([f"NULLIF({period}_{source}_DIAG, '')" for source in ["HES_APC", "HES_OP", "GDPPR"]]) + f") AS {period}_DIAG")
    cols.append("CONCAT_WS(',', " + ", ".join([f"NULLIF({period}_{source}_DIAG, '')" for source in ["HES_APC", "HES_OP"]]) + f") AS {period}_DIAG_NO_GDPPR")
  return cols

all_cols = ",\n  ".join(hes_apc_cols + hes_op_cols + gdppr_cols + create_concatenated_cols(lookback_periods.keys()))

# COMMAND ----------

lookback = spark.sql(f"""
SELECT
  a.*,
  {all_cols}
FROM {collab_database}.{input_table_name} a
INNER JOIN {collab_database}.{cohort_hes_apc_lookback_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE
INNER JOIN {collab_database}.{cohort_hes_op_lookback_table_name} c
ON a.PERSON_ID_DEID = c.PERSON_ID_DEID AND a.LOOKBACK_DATE = c.LOOKBACK_DATE
INNER JOIN {collab_database}.{cohort_gdppr_lookback_table_name} d
ON a.PERSON_ID_DEID = d.PERSON_ID_DEID AND a.LOOKBACK_DATE = d.LOOKBACK_DATE""")

print(f"Creating `{output_table_name}` with study start date == {study_start}")

lookback.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)
optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

if test:
  print("Checking no rows were lost by adding lookback info...")
  n = spark.sql(f"SELECT COUNT(*) FROM {collab_database}.{input_table_name}").first()[0]
  n_ad_look = spark.sql(f"SELECT COUNT(*) FROM {collab_database}.{output_table_name}").first()[0]
  print(n, n_ad_look)
  assert n == n_ad_look
