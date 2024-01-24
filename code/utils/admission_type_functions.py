# Databricks notebook source
# We read in admission type definitions and use them to write code that applies their hierarchical rules to a set of admissions

# COMMAND ----------

# MAGIC %run ./admission_type_definitions

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

import io

import pandas as pd


# Programmatically builds a case string for an admission type
def create_case_string(row, sep=",", old_pims=False):
    case_string = "WHEN "

    if "PIMS" in row["name"] and "NO_TYPE" not in row["name"]:
        if old_pims:
            case_string += "(PIMS_BY_CODE_OLD == 1)"
        else:
            case_string += "(PIMS_BY_CODE == 1)"

    # Using the codes in the four include/exclude columns, build up requirement statements for type definitions, where primary inclusion codes are present, note that neither primary nor secondary exclude codes can be present, similar for secondary
    primary_strings = []
    if row["includes_primary"]:
        primary_strings.append(
            f"DIAG_4_CONCAT_PRIMARY RLIKE '{convert_list_to_regex_string(row['includes_primary'], sep)}'"
        )
    if row["excludes_primary"]:
        primary_strings.append(
            f"DIAG_4_CONCAT_PRIMARY NOT RLIKE '{convert_list_to_regex_string(row['excludes_primary'], sep)}'"
        )
    if row["excludes_secondary"]:
        primary_strings.append(
            f"DIAG_4_CONCAT_SECONDARY NOT RLIKE '{convert_list_to_regex_string(row['excludes_secondary'], sep)}'"
        )
    primary_string = " AND ".join(primary_strings)

    secondary_strings = []
    if row["includes_secondary"]:
        secondary_strings.append(
            f"DIAG_4_CONCAT_SECONDARY RLIKE '{convert_list_to_regex_string(row['includes_secondary'], sep)}'"
        )
    if row["excludes_secondary"]:
        secondary_strings.append(
            f"DIAG_4_CONCAT_SECONDARY NOT RLIKE '{convert_list_to_regex_string(row['excludes_secondary'], sep)}'"
        )
    if row["excludes_primary"]:
        secondary_strings.append(
            f"DIAG_4_CONCAT_PRIMARY NOT RLIKE '{convert_list_to_regex_string(row['excludes_primary'], sep)}'"
        )
    secondary_string = " AND ".join(secondary_strings)

    # When both primary and secondary include codes are present in a type definition, we allow inclusion based on either of the strings built above being satisified, if not then BOTH must be satisfied, i.e. if there were primary include + exclude codes AND secondary exclude codes we require all of them to be satisfied, see PIMS and INCIDENTAL defns to make sense of this
    if primary_string and secondary_string and row["includes_primary"] and row["includes_secondary"]:
        case_string += f"(({primary_string}) OR ({secondary_string}))"
    elif primary_string and secondary_string and primary_string == secondary_string:
        case_string += f"({primary_string})"
    elif primary_string and secondary_string:
        case_string += f"(({primary_string}) AND ({secondary_string}))"
    elif primary_string:
        case_string += f"({primary_string})"
    elif secondary_string:
        case_string += f"({secondary_string})"

    case_string += f" THEN '{row['type']}'\n"
    return case_string


# Creates binary case when string for each type definition
def create_case_when_string(row):
    return f"CASE {row['case_statement'].split(' THEN ')[0]} THEN 1 ELSE 0 END AS {row['name']}"


# Creates an option in the overall TYPE_OF_ADMISSION categorical for each type definition
def create_categorical_definition_string(case_statements):
    return f"CASE\n    {case_statements.str.cat(sep='    ')}    ELSE 'Unknown'\n  END AS TYPE_OF_ADMISSION"


def create_binary_definitions_string(case_when_statements):
    return case_when_statements.str.cat(sep=",\n  ")


# Programmatically builds a SQL statement to apply the hierarchy of admission types via ICD-10 codes and other variables to a table of admissions
def create_typed_admissions_query(table_name, use_reinfections_definitions=False, important_only=False, old_pims=False):
    if use_reinfections_definitions:
        df = pd.read_csv(io.StringIO(admission_type_definitions_for_reinfections)).fillna(False)
    else:
        df = pd.read_csv(io.StringIO(admission_type_definitions)).fillna(False)
    if important_only:
        print("removing incidental admissions")
        df = df[df["type"] != "Incidental"]
    df["name"] = df["type"].str.replace(" ", "_").str.upper()

    df["case_statement"] = df.apply(create_case_string, old_pims=old_pims, axis=1)
    df["case_when_statement"] = df.apply(create_case_when_string, axis=1)
    type_of_admission_definition = create_categorical_definition_string(df["case_statement"])
    type_of_admission_definitions_separate = create_binary_definitions_string(df["case_when_statement"])

    query_string = f"""
SELECT
  *,
  {type_of_admission_definition},
  {type_of_admission_definitions_separate}
FROM {table_name}"""
    print(query_string)
    return query_string
