# Databricks notebook source
try:
  skip
except:
  skip = ""

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

if not skip:
  config = {
    "database": "dars_nic_391419_j3w9t",
    "dars_database": "dars_nic_391419_j3w9t_collab",
    "collab_database": "dsa_391419_j3w9t_collab",
    "project_id": "ccu029",
    "hospitalisation_start_date": None,
    "pims_reject_code_statement": convert_list_to_regex_string("A00,A01,A02,A03,A04,A05,A06,A07,A080,A081,A082,A17,A18,A19,A2,A3,A4,A5,A6,A7,A80,A810,A811,A812,A82,A83,A84,A85,A870,A871,A880,A881,A9,B01,B02,B03,B04,B05,B06,B07,B08,B15,B16,B17,B18,B2,B300,B301,B303,B330,B331,B333,B334,B340,B341,B343,B344,B4,B5,B6,B7,B8,B90,B91,B92,B94,B95,B96,B970,B971,B973,B974,B975,B976,B977,B98,C,D37,D38,D4,D5,D60,D61,D62,D63,D65,D66,D67,D8,E883,G00,G01,G02,G030,G031,G032,G041,G042,G05,G06,G07,G08,I0,I31,132,I35,I36,I37,I38,I39,Q2,J020,J030,J09,J10,J11,J120,J121,J122,J123,J13,J14,J15,J160,J17,J200,J201,J202,J203,J204,J205,J206,J207,J210,J211,J218,J36,J390,J391,J69,J85,J86,J94,J95,L0,M0,M01,M03,O,P0,P1,P21,P23,P24,P26,P350,P351,P352,P354,P354,P36,P37,P50,P51,P54,P75,P76,P77,P780,P781,P782,P783,P960,R02,R572,S,T,V,Z958,Z982,Z948"),
    "pims_defn_date": "2020-05-01",
    "preamble": "ccu029_test"
  }
