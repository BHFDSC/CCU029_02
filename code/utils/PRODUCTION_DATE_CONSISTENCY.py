# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../project_config $skip=skip

# COMMAND ----------

instantiate_config(config)

# COMMAND ----------

extract_archived_on = str(spark.sql(f"SELECT MAX(archived_on) FROM {dars_database}.hes_apc_all_years_archive").first()[0])

# COMMAND ----------

try:
  if archived_on == extract_archived_on:
    print(f"Config specifies {archived_on} as the archived-on date, this is the latest (per the hes_apc_all_years_archive).")
  else:
    print(f"Config specifies {archived_on} as the archived-on date, NOTE the latest is: {extract_archived_on}")
except:
  archived_on = extract_archived_on
  print(f"No archived-on date specified, using the latest which is: {extract_archived_on}")

# COMMAND ----------

tables = {
  "gdppr_table" : f"{preamble}_gdppr",
  "sgss_table" : f"{preamble}_sgss",
  "deaths_table" : f"{preamble}_deaths",
  "hes_apc_table" : f"{preamble}_hes_apc",
  "hes_op_table" : f"{preamble}_hes_op",
  "hes_cc_table" : f"{preamble}_hes_cc",
  "hes_ae_table" : f"{preamble}_hes_ae"
}

archives = {
  "gdppr_table" : "gdppr_dars_nic_391419_j3w9t_archive",
  "sgss_table" : "sgss_dars_nic_391419_j3w9t_archive",
  "deaths_table" : "deaths_dars_nic_391419_j3w9t_archive",
  "hes_apc_table" : "hes_apc_all_years_archive",
  "hes_op_table" : "hes_op_all_years_archive",
  "hes_cc_table" : "hes_cc_all_years_archive",
  "hes_ae_table" : "hes_ae_all_years_archive"
}


def create_base_tables():
    gdppr = spark.sql(f"""
    SELECT * FROM {dars_database}.gdppr_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'""")
    full_create(gdppr, tables['gdppr_table'], optimise=False)
    

    sgss = spark.sql(f"""
    SELECT * FROM {dars_database}.sgss_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'""")
    full_create(sgss, tables['sgss_table'], optimise=False)
    

    deaths = spark.sql(f"""
    SELECT * FROM {dars_database}.deaths_dars_nic_391419_j3w9t_archive
    WHERE archived_on = '{archived_on}'
    """)
    full_create(deaths, tables['deaths_table'], optimise=False)

    hes_apc = spark.sql(f"""
    SELECT * FROM {dars_database}.hes_apc_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    full_create(hes_apc, tables['hes_apc_table'], optimise=False)

    hes_op = spark.sql(f"""
    SELECT * FROM {dars_database}.hes_op_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    full_create(hes_op, tables['hes_op_table'], optimise=False)
    
    hes_cc = spark.sql(f"""
    SELECT * FROM {dars_database}.hes_cc_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    full_create(hes_cc, tables['hes_cc_table'], optimise=False)
    
    hes_ae = spark.sql(f"""
    SELECT * FROM {dars_database}.hes_ae_all_years_archive
    WHERE archived_on = '{archived_on}'
    """)
    full_create(hes_ae, tables['hes_ae_table'], optimise=False)
    
    for table in tables.items():
      count = spark.sql(f"SELECT * FROM {table[1]}").count()
      if not count:
        table_max_archived_on = str(spark.sql(f"SELECT MAX(archived_on) FROM {dars_database}.{archives[table[0]]}").first()[0])
        print(f"There is no archive in `{archives[table[0]]}` for the selected date (`{archived_on}`), falling back to the archive from `{table_max_archived_on}`")
        table_replacement = spark.sql(f"""
        SELECT * FROM {dars_database}.{archives[table[0]]}
        WHERE archived_on = '{table_max_archived_on}'
        """)
        full_create(table_replacement, table[1], optimise=False)

create_base_tables()

# COMMAND ----------

# try:
#   spark.sql(f"""
# CREATE OR REPLACE TEMP VIEW {preamble}_skinny AS
# SELECT * FROM {dars_database}.{preamble}_manually_compiled_skinny
# """)
#   tables["skinny_table"] = f"{preamble}_skinny"
# except:
#   print("NO SKINNY TABLE FOUND.")

print(f"""
TABLES GENERATED
{', '.join(tables.values())}
""")
