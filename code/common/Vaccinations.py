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

input_table_name = f"{preamble}_{cohort_w_deaths_output_table_name}"
output_table_name = f"{preamble}_{cohort_w_vaccinations_output_table_name}"

print("Subsetting vaccination info to IDs from cohort and extracting dates...")

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_filtered_vax AS
SELECT b.INFECTION_DATE, a.*
FROM dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t a
INNER JOIN {collab_database}.{input_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID"""
)
spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_filtered_vax_dates AS
SELECT
  vax1.PERSON_ID_DEID,
  vax1.INFECTION_DATE,
  vax1.date_vax AS FIRST_VAX_DATE,
  vax2.date_vax AS SECOND_VAX_DATE,
  booster.date_vax AS BOOSTER_VAX_DATE
FROM (
  SELECT
    PERSON_ID_DEID,
    INFECTION_DATE,
    MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
  FROM {preamble}_filtered_vax
  WHERE VACCINATION_PROCEDURE_CODE = 1324681000000101
GROUP BY PERSON_ID_DEID, INFECTION_DATE
) as vax1
LEFT JOIN (
  SELECT
    PERSON_ID_DEID,
    INFECTION_DATE,
    MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
  FROM {preamble}_filtered_vax
  WHERE VACCINATION_PROCEDURE_CODE = 1324691000000104
  GROUP BY PERSON_ID_DEID, INFECTION_DATE
) as vax2
ON vax1.PERSON_ID_DEID = vax2.PERSON_ID_DEID AND vax1.INFECTION_DATE = vax2.INFECTION_DATE
LEFT JOIN (
  SELECT
    PERSON_ID_DEID,
    INFECTION_DATE,
    MIN(to_date(substring(DATE_AND_TIME, 0,8), "yyyyMMdd")) as date_vax
  FROM {preamble}_filtered_vax
  WHERE VACCINATION_PROCEDURE_CODE = 1362591000000103
  GROUP BY PERSON_ID_DEID, INFECTION_DATE
) as booster
ON vax1.PERSON_ID_DEID = booster.PERSON_ID_DEID AND vax1.INFECTION_DATE = booster.INFECTION_DATE"""
)

# COMMAND ----------

if test:
    display(spark.sql(f"SELECT * FROM {preamble}_filtered_vax_dates ORDER BY PERSON_ID_DEID, INFECTION_DATE ASC"))

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW {preamble}_cohort_w_vaccinations AS
SELECT
  cohort.*,
  CASE
    WHEN DATE_ADD(BOOSTER_VAX_DATE, 14) <= cohort.INFECTION_DATE THEN "Booster Dose"
    WHEN DATE_ADD(SECOND_VAX_DATE, 14) <= cohort.INFECTION_DATE THEN "Second Dose"
    WHEN DATE_ADD(FIRST_VAX_DATE, 14) <= cohort.INFECTION_DATE THEN "First Dose"
    ELSE "Unvaccinated"
  END AS VACCINATION_STATUS_14_DAYS_PRIOR_TO_INFECTION,
  CASE WHEN DATE_ADD(FIRST_VAX_DATE, 14) <= cohort.INFECTION_DATE THEN 1 ELSE 0 END AS VACCINATED_14_DAYS_PRIOR_TO_INFECTION,
  FIRST_VAX_DATE,
  SECOND_VAX_DATE,
  BOOSTER_VAX_DATE
FROM {collab_database}.{input_table_name} as cohort
LEFT JOIN {preamble}_filtered_vax_dates as vax
ON cohort.PERSON_ID_DEID = vax.PERSON_ID_DEID AND cohort.INFECTION_DATE = vax.INFECTION_DATE"""
)

# COMMAND ----------

cohort_w_vaccinations = spark.sql(f"SELECT * FROM {preamble}_cohort_w_vaccinations")

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_vaccinations.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
