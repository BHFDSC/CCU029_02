# Databricks notebook source
try:
  skip
except:
  skip = ""

# COMMAND ----------

# MAGIC %run ../../common/project_config

# COMMAND ----------

if not skip:
  config.update(
    {
      "question_id": "02",
      "archived_on": "2023-11-23",
      "include_reinfections": True,
      "admissions_only": False,
      "no_gdppr_uhcs": True,
      "study_start": "2020-07-01",
      "study_end": "2023-09-30",
      "hospitalisation_start_date": "2020-01-01",
      "case_hospitalisation_window": "42",
      "infection_period_length": "90",
      "lookback_periods": {"apa": 0, "nine_months": 9, "two_years": 24, "five_years": 60},
    }
  )
  config.update(
    {
      "preamble": config["project_id"] + "_" + config["question_id"],
      "primary_codes": "U071|U072" if config["include_reinfections"] else "U071|U072|U073|U074",
      "infection_censoring_date": datetime.strftime(datetime.strptime(config["study_end"], "%Y-%m-%d") - timedelta(days=int(config["case_hospitalisation_window"])), "%Y-%m-%d"),
      "variant_period_dict": {
        '2020-12-07' : 'Pre-Alpha: 2020-07-01 to 2020-12-07',
        '2021-05-17' : 'Alpha: 2020-12-08 to 2021-05-17',
        '2021-12-13' : 'Delta: 2021-05-18 to 2021-12-13',
        '2022-07-13' : 'Omicron 1: 2021-12-14 to 2022-07-13',
        '2023-02-13' : 'Omicron 2: 2022-07-14 to 2023-02-13',
        config["study_end"] : 'Omicron 3: 2023-02-14 to 2023-09-30'
      },
    }
  )
