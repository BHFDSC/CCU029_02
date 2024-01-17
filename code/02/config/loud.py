# Databricks notebook source
# MAGIC %run ./quiet

# COMMAND ----------

print(f"""
Project ID (`project_id`): {config["project_id"]}
Question (`question_id`): {config["question_id"]}

Study Cohort Start Date (`study_start`): {config["study_start"]}
Study Cohort End Date (`study_end`): {config["study_end"]}
Archive Date (`archived_on`): {config["archived_on"]}

Include Reinfections (`include_reinfections`): {config["include_reinfections"]}
Infection Period Length (`infection_period_length`): {config["infection_period_length"]}
Case Hospitalisation Window (`case_hospitalisation_window`): {config["case_hospitalisation_window"]}

Variant Periods (`variant_period_dict`): {config["variant_period_dict"]}
""")
