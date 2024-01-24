# Databricks notebook source
# Convert from ethnicity codes to groups and descriptions, and convert numeric SEX to Male / Female

import io
from itertools import chain

import pandas as pd
from pyspark.sql.functions import col, create_map, lit

# From HES APC Data Dictionary (ONS 2011 census categories)
dict_ethnic = """
Code,Group,Description
A,White,British (White)
B,White,Irish (White)
C,White,Any other White background
D,Mixed,White and Black Caribbean (Mixed)
E,Mixed,White and Black African (Mixed)
F,Mixed,White and Asian (Mixed)
G,Mixed,Any other Mixed background
H,Asian or Asian British,Indian (Asian or Asian British)
J,Asian or Asian British,Pakistani (Asian or Asian British)
K,Asian or Asian British,Bangladeshi (Asian or Asian British)
L,Asian or Asian British,Any other Asian background
M,Black or Black British,Caribbean (Black or Black British)
N,Black or Black British,African (Black or Black British)
P,Black or Black British,Any other Black background
R,Chinese,Chinese (other ethnic group)
S,Other,Any other ethnic group
T,Unknown,Unknown
W,Unknown,Unknown
Z,Unknown,Unknown
X,Unknown,Unknown
99,Unknown,Unknown
0,White,White
1,Black or Black British,Black - Caribbean
2,Black or Black British,Black - African
3,Black or Black British,Black - Other
4,Asian or Asian British,Indian
5,Asian or Asian British,Pakistani
6,Asian or Asian British,Bangladeshi
7,Chinese,Chinese
8,Other,Any other ethnic group
9,Unknown,Unknown
"""

dict_ethnic = pd.read_csv(io.StringIO(dict_ethnic))
dict_ethnic = dict_ethnic.rename(columns={"Code": "code", "Group": "ethnic_group", "Description": "ethnicity"})

# Convert to dictionary for mapping
mapping_ethnic_group = dict(zip(dict_ethnic["code"], dict_ethnic["ethnic_group"]))
mapping_ethnicity = dict(zip(dict_ethnic["code"], dict_ethnic["ethnicity"]))
mapping_sex = {1: "Male", 2: "Female"}

mapping_expr_ethnic_group = create_map([lit(x) for x in chain(*mapping_ethnic_group.items())])
mapping_expr_ethnicity = create_map([lit(x) for x in chain(*mapping_ethnicity.items())])
mapping_expr_sex = create_map([lit(x) for x in chain(*mapping_sex.items())])
