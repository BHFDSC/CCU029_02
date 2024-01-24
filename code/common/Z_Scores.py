# Databricks notebook source
skip = "Yes" if "config" in locals() else ""

# COMMAND ----------

# MAGIC %run ../01/config/quiet $skip=skip

# COMMAND ----------

unpack_config(config)

# COMMAND ----------

# MAGIC %run ./utils/TABLE_NAMES

# COMMAND ----------

# MAGIC %run ./utils/lms2z

# COMMAND ----------

try:
    test
except:
    test = True

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

input_table_name = f"{preamble}_{cohort_w_bmi_output_table_name}"
cohort_bmi_zscores_table_name = f"{preamble}_{cohort_bmi_zscores_table_name}"
# dbutils.widgets.text("cohort_bmi_zscores_table_name", cohort_bmi_zscores_table_name)
cohort_weight_zscores_table_name = f"{preamble}_{cohort_weight_zscores_table_name}"
# dbutils.widgets.text("cohort_weight_zscores_table_name", cohort_weight_zscores_table_name)
output_table_name = f"{preamble}_{cohort_w_zscores_output_table_name}"

print("Calculate Z-Scores relative to BMI- and weight-for-age...")

# COMMAND ----------

df = spark.sql(
    f"SELECT PERSON_ID_DEID, LOOKBACK_DATE, AGE, BMI, WEIGHT, SEX FROM {collab_database}.{input_table_name} ORDER BY PERSON_ID_DEID, LOOKBACK_DATE"
).toPandas()
ids = list(df["PERSON_ID_DEID"])
ids2 = list(df["LOOKBACK_DATE"])
x = np.array(df["AGE"])
bmi = np.array(df["BMI"])
weight = np.array(df["WEIGHT"])
sex = np.array(df["SEX"]).astype(int)

# COMMAND ----------

bmi_z_scores = LMS2z(x, bmi, sex, "bmi", toz=True)
weight_z_scores = LMS2z(x, weight, sex, "wt", toz=True)

# COMMAND ----------

bmi_z_scores_df = spark.createDataFrame(
    pd.DataFrame({"PERSON_ID_DEID": ids, "LOOKBACK_DATE": ids2, "BMI_Z_SCORE": bmi_z_scores})
).replace(float("nan"), None)
weight_z_scores_df = spark.createDataFrame(
    pd.DataFrame({"PERSON_ID_DEID": ids, "LOOKBACK_DATE": ids2, "WEIGHT_Z_SCORE": weight_z_scores})
).replace(float("nan"), None)

# COMMAND ----------

bmi_z_scores_df.createOrReplaceTempView(cohort_bmi_zscores_table_name)
drop_table(cohort_bmi_zscores_table_name)
create_table(cohort_bmi_zscores_table_name)

# COMMAND ----------

weight_z_scores_df.createOrReplaceTempView(cohort_weight_zscores_table_name)
drop_table(cohort_weight_zscores_table_name)
create_table(cohort_weight_zscores_table_name)

# COMMAND ----------

cohort_w_z_scores = spark.sql(
    f"""
SELECT
  a.*,
  BMI_Z_SCORE,
  WEIGHT_Z_SCORE
FROM {collab_database}.{input_table_name} a
INNER JOIN {collab_database}.{cohort_bmi_zscores_table_name} b
ON a.PERSON_ID_DEID = b.PERSON_ID_DEID AND a.LOOKBACK_DATE = b.LOOKBACK_DATE
INNER JOIN {collab_database}.{cohort_weight_zscores_table_name} c
ON a.PERSON_ID_DEID = c.PERSON_ID_DEID AND a.LOOKBACK_DATE = c.LOOKBACK_DATE
"""
)

print(f"Creating `{output_table_name}` with study start date == {study_start}")

cohort_w_z_scores.createOrReplaceTempView(output_table_name)
drop_table(output_table_name)
create_table(output_table_name)

table = spark.sql(f"SELECT * FROM {collab_database}.{output_table_name}")
print(f"`{output_table_name}` has {table.count()} rows and {len(table.columns)} columns.")
