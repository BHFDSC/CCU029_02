# Databricks notebook source
from datetime import datetime, timedelta

# COMMAND ----------

def unpack_config(config: dict):
  for name, value in config.items():
    globals()[name] = value
    

def instantiate_config(config: dict):
  for name, value in config.items():
    globals()[name] = value
#     if type(value) == str:
#       dbutils.widgets.text(name, value)
    

def get_col_names(table, without = []):
  return ", ".join([x for x in spark.sql(f"SELECT * FROM {table} LIMIT 1").schema.names if x not in without])


def unique_and_filter(sequence, to_remove):
  filtered_sequence = [x for x in sequence if x not in to_remove]
  seen = set()
  return [x for x in filtered_sequence if not (x in seen or seen.add(x))]


# Converts a standard comma separated list string into regex union, formatting requirements safely allow for incomplete code stems to be used, i.e. "C1" for all codes starting with those two characters
def convert_list_to_regex_string(input_string, sep=","):
  return f"{'|'.join(['^' + x for x in input_string.split(sep)] + [',' + x for x in input_string.split(sep)])}"


def generate_variant_period_case_when(date_var, variant_period_dict):
  periods = "\n  ".join([f"WHEN {date_var} <= '{date}' THEN '{name}'" for date, name in variant_period_dict.items()])
  return f"CASE\n  {periods}\n  ELSE 'ERROR'\nEND"


# Functions to create and drop (separate) a table from a pyspark data frame
# Builds delta tables to allow for optimisation
# Modifies owner to allow tables to be dropped
def create_table(table_name: str, database_name='dsa_391419_j3w9t_collab', if_not_exists=True) -> None:
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
  spark.sql(f"""CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{database_name}.{table_name} USING DELTA AS
  SELECT * FROM {table_name}""")
#   spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  

def drop_table(table_name: str, database_name='dsa_391419_j3w9t_collab', if_exists=True):
  spark.sql(f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{database_name}.{table_name}")


def optimise_table(table_name: str, order_by: str):
  spark.sql(f"OPTIMIZE dsa_391419_j3w9t_collab.{table_name}{' ZORDER BY ' + order_by if order_by else ''}")
  

def full_create(table, table_name: str, optimise: bool = True, optimise_column: str = "PERSON_ID_DEID"):
  print(f"Creating {table_name}...")
  table.createOrReplaceTempView(table_name)
  drop_table(table_name)
  create_table(table_name)
  if optimise:
    optimise_table(table_name, optimise_column)


def row_sum_across(*cols):
  return reduce(add, cols, lit(0))

