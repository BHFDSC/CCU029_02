# Databricks notebook source
conditions = {
    "Interest": """name,icd_include,icd_exclude
Seizures,"G400,G401,G402,G403,G404,G405,G406,G407,G408,G409,R560,R568,P90",
Strokes,"G463,G464,I630,I631,I632,I633,I634,I635,I636,I638,I639,I64,I65,I679,I693,I694",
Hepatitis and Liver Failure,"B178,B179,B19,K720,K711,K752,K754,K758,K759","B15,B16,B17,B18,B258,B259,B27"
Encephalitis and CNS,"A858,A86,A878,A879,A89,B941,G040,G049,G370","A178,A321,A398,A80,A81,A82,A83,A84,A850,A851,A852,A870,A871,A872,A922,B004,B011,B020,B050,B060,B262,G042"
Cardiac Events,"I460,I499,I470,I472,I490,P291",
Appendicitis,"K35,K36,K37",
Type One Diabetes,"E10,E13,E14,P702",
Type Two Diabetes,E11,
ANCA and Inflammatory Vasculitis,"D891,M052,I776,I778,J998",
Polymyositis Dermatomyositis,"M332,M339",
Systemic Lupus Erythematosus,"M32,G058,M737,I328,I682,J991,N085,N164,G635",
Scleroderma,"M34,G737,J991,L940,L941",
Hashimoto Thyroiditis,E063,
Addisons Disease,E271,
IGA Nephropathy,"N023,N028",
Inflammatory Bowel disease,"K50,K51",
Juvenile Rheumatoid Arthritis,M080,
Coeliac Disease,K900,
Pancreatitis,"K850,K858,K859",""",
    "Placebo": """name,icd_include,icd_exclude
Wrist Fracture,S62,
Testicular Torsion,N44,
Ovarian Torsion,"N835,Q502",
Inguinal Hernia,K40,
Inhaled Foreign Body,"W79,W80,T17",
Ingested Foreign Body,T18,""",
}

# COMMAND ----------
