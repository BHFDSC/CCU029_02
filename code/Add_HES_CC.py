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

input_table_name=f"{preamble}_{admissions_output_table_name}"
output_table_name=f"{preamble}_{admissions_w_icu_output_table_name}"

# COMMAND ----------

# # 1. Create ICU admission flag

# **HES CC**
# > Critical Care is a subset of APC data. It consists of Adult Critical Care from 2008-09 onwards (patients treated on adult critical care units), with Neonatal and Paediatric Critical Care included from 2017-18.   
# > The field Critical Care Period Type (CCPERTYPE) can be used to differentiate between the different types of critical care record.  
# > Each adult CC record represents a critical care period (from admission to a critical care location to discharge from that location).  
# > Each neonatal and paediatric CC record represents a calendar day (or part thereof) of neonatal or paediatric critical care.  


# **CCPERTYPE: Critical Care Period Type**  
# 01 = Adult (adult facilities, patients >= 19 years old on admission predominate)  
# 02 = Paediatric (children and young people facilities, patients ? 29 days to <19 years predominate)  
# 03 = Neonatal (neonatal facilities, patients <29 days on admission predominate)  
  
# **BESTMATCH**  
# > A flag stating whether the row represents the best match between the critical care and episode start and end dates for this critical care period. This flag is used to limit the data in instances where there is more than one row per critical care period. See Appendix C in the Critical Care 2008-09 publication for further details.  
# Value:  
# * Y or 1 = Row represents the best match between the critical care and episode dates
# * N or NULL = Row doesn?t represent the best match between the critical care and episode date

# Reference: [HES Technical Output Specification](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)

# COMMAND ----------

cohort_ICU = spark.sql(f"""
SELECT icu.ICU, cohort.*
FROM {collab_database}.{input_table_name} as cohort
LEFT JOIN (
  SELECT DISTINCT
    cohort_ids.PERSON_ID_DEID,
    cohort_ids.ADMIDATE,
    1 as ICU
  FROM (
    SELECT PERSON_ID_DEID, ADMIDATE, DISDATE
    FROM {collab_database}.{input_table_name}
  ) as cohort_ids
  INNER JOIN (
    SELECT PERSON_ID_DEID, to_date(CCSTARTDATE, 'yyyyMMdd') as CCSTARTDATE
    FROM {collab_database}.ccu029_manually_compiled_hes_cc
    WHERE
      (CCPERTYPE = 2 OR CCPERTYPE = 3) -- PICU or NICU
      AND (BESTMATCH = 1)
  ) as cc
  ON 
    cohort_ids.PERSON_ID_DEID = cc.PERSON_ID_DEID
    AND cc.CCSTARTDATE >= cohort_ids.ADMIDATE
    AND (cc.CCSTARTDATE <= cohort_ids.DISDATE OR cohort_ids.DISDATE IS NULL)
) as icu
ON cohort.PERSON_ID_DEID = icu.PERSON_ID_DEID AND cohort.ADMIDATE = icu.ADMIDATE
""")

if test:
  display(cohort_ICU)

print("Defining ICU Treatments...")

# COMMAND ----------

# # 2. ICU Treatments

# Uses Critical Care Activity Codes (`CCACTCODEn`)
# >A type of critical care activity provided to a patient during a critical care period. Up to 20 activity codes can be submitted on each daily neonatal/paediatric critical care record.  
  
# The dictionary to decode these is created in [`~/CCU029/auxiliary/lookups/CCACTODE`](https://db.core.data.digital.nhs.uk/#notebook/5445633/command/5445666) and stored in `dars_nic_391419_j3w9t_collab.ccu029_lkp_ccactcode`

# Plan:
# 1. Pivot all `CCACTCODEn` into `ID | CCACTIVDATE | CCACTCODE` + Join to a custom dictionary to decode CCACTCODE
# 2. Join to cohort with date conditions to ensure CCACTCODEs occurred within hospital admissions
# 3. Pivot to produce binary feature matrix
# 4. Join onto cohort to add covariates

# COMMAND ----------

# 2.1 Pivot all `CCATCODEn` to skinny record and join dictionary

# COMMAND ----------

import pyspark.sql.functions as f

# Load HES CC where admissions in PICU or NICU (CCPERTYPE = 2 | 3)
cc = spark.sql(f"""
SELECT *
FROM {collab_database}.ccu029_manually_compiled_hes_cc
WHERE 
  (CCPERTYPE = 2 OR CCPERTYPE = 3)
  AND BESTMATCH = 1
""")

# Get CCACTCODEn cols (Thanks to Jake Kasan at NHS Digital for nice regex pivot script)
CCACTCODE_fields = [c for c in cc.columns if c.startswith("CCACTCODE")]
# Unlikley to use code position for this analysis but include nonetheless
CCACTCODE_and_n = [(c, int(c.lstrip("CCACTCODE"))) for c in CCACTCODE_fields]
CCACTCODE_columns = f.array(*(f.struct(f.col(c).alias("value"), f.lit(n).alias("n")) for c, n in CCACTCODE_and_n))

# Pivot
ccact_skinny = (
  cc
  .select("PERSON_ID_DEID",
          f.to_date(f.col("CCACTIVDATE"), "yyyyMMdd").alias("CCACTIVDATE"),
          f.explode(CCACTCODE_columns).alias("CCACTCODE"))
  .selectExpr("PERSON_ID_DEID", "CCACTIVDATE", "CCACTCODE.value as CCACTCODE", "CCACTCODE.n as CCACTCODEn")
  .filter("CCACTCODE IS NOT NULL")
  .withColumn('value', f.lit(1))
)

# Join onto custom dictionary
lkp_CCACTCODE = spark.sql("SELECT CCACTCODE, Short as CCACTCODE_desc FROM dars_nic_391419_j3w9t_collab.ccu029_lkp_ccactcode")
ccact_skinny = (ccact_skinny
            .join(lkp_CCACTCODE, "CCACTCODE", "left")
            # Use alias for ID to avoid duplication in join
            .selectExpr("PERSON_ID_DEID as ID_hosp_CC", "CCACTIVDATE", "CCACTCODE_desc as CCACTCODE", "value")
           )

if test:
  display(ccact_skinny)

# COMMAND ----------

# # 2.2 Join skinny to cohort with date conditions

# * Ensures that CCACTCODEs took place *within* hospital admission
# * WHERE CCACTIVDATE >= ADMIDATE AND CCACTIVDATE <= DISDATE

# COMMAND ----------

cohort_ids = spark.sql(f"""
SELECT
  PERSON_ID_DEID,
  ADMIDATE,
  DISDATE
FROM
  {collab_database}.{input_table_name}
""")

cc_cohort = (cohort_ids
             .join(ccact_skinny, 
                ((cohort_ids.PERSON_ID_DEID == ccact_skinny.ID_hosp_CC) & (ccact_skinny.CCACTIVDATE >= cohort_ids.ADMIDATE) & ((ccact_skinny.CCACTIVDATE <= cohort_ids.DISDATE) | cohort_ids.DISDATE.isNull())), 
                # Use inner join at this stage
                "inner")
             # Keep date cols for visual inspection
             .drop("ID_hosp_CC")
         )

# COMMAND ----------

# 2.3. Pivot into binary feature matrix

# COMMAND ----------

import numpy as np
np.bool = np.bool_
np.float = np.float_
np.int = np.int_

# COMMAND ----------

import databricks.koalas as ks

X = (cc_cohort
     .drop("DISDATE", "CCACTIVDATE")
     .to_koalas()
     .pivot_table(
       index=['PERSON_ID_DEID', 'ADMIDATE'], 
       columns='CCACTCODE', 
       values='value')
     .fillna(0)
     .reset_index()
     .to_spark()
     # Create an ICU definition from presence of CCACTCODEs
     .withColumn('ICU_CCACTIV', f.lit(1))
)

# COMMAND ----------

# # 2.4. Join onto main cohort table

# * `cohort_ICU` is the main cohort with binary ICU admission flag, as generated in Section 1

# COMMAND ----------

sub_cohort_w_icu = (
  cohort_ICU.select(["PERSON_ID_DEID", "ADMIDATE", "ICU"])
  .join(X, ["PERSON_ID_DEID", "ADMIDATE"], "left")
  .fillna(0, subset = X.schema.names + ["ICU"])
)
cohort_w_icu = spark.sql(f"SELECT * FROM {collab_database}.{input_table_name}").join(sub_cohort_w_icu, ["PERSON_ID_DEID", "ADMIDATE"], "left")

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_icu.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

optimise_table(output_table_name, "PERSON_ID_DEID")

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR(ADMIDATE), MONTH(ADMIDATE), COUNT(*)
# MAGIC FROM dsa_391419_j3w9t_collab.ccu029_02_wip_admissions_w_icu
# MAGIC WHERE ICU == 1
# MAGIC GROUP BY YEAR(ADMIDATE), MONTH(ADMIDATE)
# MAGIC ORDER BY YEAR(ADMIDATE) DESC, MONTH(ADMIDATE) DESC

# COMMAND ----------


