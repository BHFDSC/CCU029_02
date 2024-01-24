# Databricks notebook source
# MAGIC %run ./config/loud

# COMMAND ----------

test = False

# COMMAND ----------

# MAGIC %md
# MAGIC # HES filtering and base cohort creation

# COMMAND ----------

# MAGIC %run ../common/HES_APC_Setup $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding ICU information from HES CC

# COMMAND ----------

# MAGIC %run ../common/Add_HES_CC $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying typing protocols to admissions

# COMMAND ----------

# MAGIC %run ../common/Typing_Admissions $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter Test Data from SGSS

# COMMAND ----------

# MAGIC %run ../common/SGSS_Filtering $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC # Unify First Tests and All Admissions into First Infections with Admissions

# COMMAND ----------

# MAGIC %run ../common/Identify_All_Infections $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC # Looking back through All Prior Admissions (APA)
# MAGIC
# MAGIC As well as those in the periods nine months, two years and five years prior to admission for specific UHC definitions

# COMMAND ----------

# MAGIC %run ../common/APA $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Determining BMI and Z-Scores

# COMMAND ----------

# MAGIC %run ../common/BMI $config=config

# COMMAND ----------

# MAGIC %run ../common/Z_Scores $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identifying Kate-defined and Green Book UHCs

# COMMAND ----------

# MAGIC %run ../common/UHCs $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC # Add auxiliary data
# MAGIC
# MAGIC ### Joining Civil Registration of Death (ONS)
# MAGIC
# MAGIC Date of death and all causes

# COMMAND ----------

# MAGIC %run ../common/Deaths $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vaccination Status (NHSD)

# COMMAND ----------

# MAGIC %run ../common/Vaccinations $config=config

# COMMAND ----------

# MAGIC %md
# MAGIC # Finalising the cohort

# COMMAND ----------

# MAGIC %run ../common/Finalise_Cohort_and_Summarise $config=config

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   AGE_CAT,
# MAGIC   POPULATION,
# MAGIC   YEAR(INFECTION_DATE),
# MAGIC   SUM(COVID_PRIMARY_COD),
# MAGIC   100000 * SUM(COVID_PRIMARY_COD) / POPULATION,
# MAGIC   AGE_CAT_ORDER
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN AGE_CAT == "< 1" THEN 578988
# MAGIC       WHEN AGE_CAT == "1 - 4" THEN 2497962
# MAGIC       WHEN AGE_CAT == "5 - 11" THEN 4730942
# MAGIC       WHEN AGE_CAT == "12 - 15" THEN 2675202
# MAGIC       ELSE 1291508
# MAGIC     END AS POPULATION,
# MAGIC     CASE
# MAGIC       WHEN AGE_CAT == "< 1" THEN 1
# MAGIC       WHEN AGE_CAT == "1 - 4" THEN 2
# MAGIC       WHEN AGE_CAT == "5 - 11" THEN 3
# MAGIC       WHEN AGE_CAT == "12 - 15" THEN 4
# MAGIC       ELSE 5
# MAGIC     END AS AGE_CAT_ORDER
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN AGE < 1 THEN "< 1"
# MAGIC       WHEN AGE < 5 THEN "1 - 4"
# MAGIC       WHEN AGE < 10 THEN "5 - 9"
# MAGIC       WHEN AGE < 15 THEN "10 - 14"
# MAGIC       WHEN AGE < 18 THEN "15 - 17"
# MAGIC       ELSE "ERROR"
# MAGIC     END AS SETH_AGE_CAT
# MAGIC     FROM dsa_391419_j3w9t_collab.ccu029_02_complete_cohort
# MAGIC   )
# MAGIC )
# MAGIC GROUP BY YEAR(INFECTION_DATE), AGE_CAT, AGE_CAT_ORDER, POPULATION
# MAGIC ORDER BY AGE_CAT_ORDER, YEAR(INFECTION_DATE) ASC

# COMMAND ----------

# MAGIC %run ../common/utils/TABLE_NAMES

# COMMAND ----------

drop_all_tables(intermediary_tables, preamble)
