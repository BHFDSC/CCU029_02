# Databricks notebook source
# MAGIC %run ./condition_definitions

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

import io
import pandas as pd
from functools import reduce
from operator import add
from pyspark.sql.functions import lit, col, when


# Programmatically builds the required SQL snippets to identify a condition via rules extracted from the include + exclude codes as well as its type and name
def create_condition_statement(name, includes, excludes, condition_type, sep=","):
  filter_string = "\n  CASE WHEN "
  icd_strings = []
  if includes:
    icd_strings.append(f"(ALL_CODES RLIKE '{convert_list_to_regex_string(includes, sep)}')")
  if excludes:
    icd_strings.append(f"(ALL_CODES NOT RLIKE '{convert_list_to_regex_string(includes, sep)}')")
#   if snomed_filter:
#     snomed_string = f"(SNOMED_CODES RLIKE '{}')"
  filter_string += " AND ".join(icd_strings)
#   if icd_strings and snomed_filter:
#     filter_string += " AND ".join(icd_strings) + " OR " + snomed_string
#   if snomed_string:
#     filter_string += snomed_string  
  return filter_string + f" THEN 1 ELSE 0 END AS {condition_type}_{name}"


def generate_full_conditions_query(df, input_table, condition_type, sep=","):
  statements = [create_condition_statement(*row, condition_type, sep) for row in zip(df['name'], df['icd_include'], df['icd_exclude'])]
  query = f"""
SELECT
  *,{','.join(statements)}
FROM {input_table}"""
  print(query)
  return query


def formulate_conditions(condition_type, input_table):
  df = pd.read_csv(io.StringIO(conditions[condition_type])).fillna(False)
  df["name"] = df["name"].str.replace(" ", "_").str.upper()
  cohort = spark.sql(generate_full_conditions_query(df, input_table, f"{condition_type.upper()}_CONDITION"))
  cols = [col(f"{condition_type.upper()}_CONDITION_{name}") for name in df["name"]]
  return cohort, cols

# COMMAND ----------

def select_first_instance_of_conditions(input_table):
  df = pd.read_csv(io.StringIO(conditions["Interest"])).fillna(False)
  statement_parts = [
    f"""SELECT
  *, "{name}" AS EVENT_CONDITION, "Interest" AS EVENT_TYPE
FROM (
  SELECT
    a.PERSON_ID_DEID,
    a.EVENT_DATE,
    a.EVENT_SOURCE,
    GREATEST(0, DATEDIFF(EVENT_DATE, DATE_OF_BIRTH) / 365.25) AS AGE
  FROM {collab_database}.{input_table_name} a
  INNER JOIN (
    SELECT PERSON_ID_DEID, MIN(EVENT_DATE) AS FIRST_EVENT_DATE
    FROM {collab_database}.{input_table_name}
    WHERE INTEREST_CONDITION_{name.replace(' ', '_').upper()} == 1
    GROUP BY PERSON_ID_DEID
  ) b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.EVENT_DATE == b.FIRST_EVENT_DATE
)
WHERE EVENT_DATE > '{study_start}' AND AGE < 18"""
  for name in df["name"]]
  df = pd.read_csv(io.StringIO(conditions["Placebo"])).fillna(False)
  statement_parts += [
    f"""SELECT
  *, "{name}" AS EVENT_CONDITION, "Placebo" AS EVENT_TYPE
FROM (
  SELECT
    a.PERSON_ID_DEID,
    a.EVENT_DATE,
    a.EVENT_SOURCE,
    GREATEST(0, DATEDIFF(EVENT_DATE, DATE_OF_BIRTH) / 365.25) AS AGE
  FROM {collab_database}.{input_table_name} a
  INNER JOIN (
    SELECT PERSON_ID_DEID, MIN(EVENT_DATE) AS FIRST_EVENT_DATE
    FROM {collab_database}.{input_table_name}
    WHERE PLACEBO_CONDITION_{name.replace(' ', '_').upper()} == 1
    GROUP BY PERSON_ID_DEID
  ) b
  ON a.PERSON_ID_DEID == b.PERSON_ID_DEID AND a.EVENT_DATE == b.FIRST_EVENT_DATE
)
WHERE EVENT_DATE > '{study_start}' AND AGE < 18"""
  for name in df["name"]]
  statement = "\nUNION ALL\n".join(statement_parts)
  print(statement)
  output = spark.sql(statement)
  return output