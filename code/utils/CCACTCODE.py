# Databricks notebook source
# MAGIC %md
# MAGIC # Create `CCACTCODE` lookup
# MAGIC > A type of critical care activity provided to a patient during a critical care period. Up to 20 activity codes can be submitted on each daily neonatal/paediatric critical care record.
# MAGIC
# MAGIC Reference: [HES Technical Output Specification](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)
# MAGIC <br>
# MAGIC * Pre-processing via python script (outside TRE) to parse `.csv` out of single cell (K10) in `HES+TOS+V1.5+-+published+M8.xlsx`

# COMMAND ----------

# MAGIC %run ./util_functions

# COMMAND ----------

csv = """
CCACTCODE,Description,Short
1,Respiratory support via a tracheal tube (Respiratory support via a tracheal tube provided),CC_tracheal_tube
2,Nasal Continuous Positive Airway Pressure (nCPAP) (patient receiving nCPAP for any part of the day),CC_nCPAP
3,Surgery (patient received surgery),CC_surgery
4,Exchange Transfusion (patient received exchange transfusion),CC_exchange_transfusion
5,Peritoneal Dialysis (patient received Peritoneal Dialysis),CC_peritoneal_dialysis
6,"Continuous infusion of inotrope, pulmonary vasodilator or prostaglandin (patient received a continuous infusion of an inotrope, vasodilator (includes pulmonary vasodilators) or prostaglandin)",CC_inodilator
7,Parenteral Nutrition (patient receiving Parenteral Nutrition (amino acids +/- lipids)),CC_PN
8,Convulsions (patient having convulsions requiring treatment),CC_convulsions
9,Oxygen Therapy (patient receiving additional oxygen),CC_O2
10,Neonatal abstinence syndrome (patient receiving drug treatment for neonatal abstinence (withdrawal) syndrome),CC_neo_abstinence
11,Care of an intra-arterial catheter or chest drain (patient receiving care of an intra-arterial catheter or chest drain),CC_art_line_or_chest_drain
12,Dilution Exchange Transfusion (patient received Dilution Exchange Transfusion),CC_dilution_exchange_transfusion
13,Tracheostomy cared for by nursing staff (patient receiving care of tracheostomy cared for by nursing staff not by an external Carer (e.g. parent)),CC_trache_nurse
14,Tracheostomy cared for by external Carer (patient receiving care of tracheostomy cared for by an external Carer (e.g. parent) not by a NURSE),CC_trache_carer
15,"Recurrent apnoea (patient has recurrent apnoea needing frequent intervention, i.e. over 5 stimulations in 8 hours, or resuscitation with IPPV two or more times in 24 hours)",CC_recurrent_apnoea
16,Haemofiltration (patient received Haemofiltration),CC_haemofiltration
21,"Carer Resident - Caring for Baby (External Carer (for example, parent) resident with the baby and reducing nursing required by caring for the baby)",CC_carer
22,"Continuous monitoring (patient requiring continuous monitoring (by mechanical monitoring equipment) of respiration or heart rate, or by transcutaneous transducers or by Saturation Monitors. Note: apnoea alarms and monitors are excluded as forms of continuous monitoring)",CC_continuous_monitoring
23,Intravenous glucose and electrolyte solutions (patient being given intravenous glucose and electrolyte solutions),CC_IV_glucose_electrolytes
24,Tube-fed (patient being tube-fed),CC_tube_fed
25,Barrier nursed (patient being barrier nursed),CC_barrier_nursed
26,Phototherapy (patient receiving phototherapy),CC_phototherapy
27,Special monitoring (patient receiving special monitoring of blood glucose or serum bilirubin measurement at a minimum frequency of more than one per calendar day),CC_special_monitoring
28,"Observations at regular intervals (patient requiring recorded observations for Temperature, Heart Rate, Respiratory Rate, Blood Pressure or scoring for neonatal abstinence syndrome. Recorded observations must be at a minimum frequency of 4 hourly)",CC_regular_obs
29,Intravenous medication (patient receiving intravenous medication),CC_IVs
50,Continuous electrocardiogram monitoring,CC_ECG_monitoring
51,Invasive ventilation via endotracheal tube,CC_IMV_ETT
52,Invasive ventilation via tracheostomy tube,CC_IMV_trache
53,Non-invasive ventilatory support,CC_NIV
55,Nasopharyngeal airway,CC_NPA
56,Advanced ventilatory support (Jet or Oscillatory ventilation),CC_jet_oscillator
57,Upper airway obstruction requiring nebulised Epinephrine/ Adrenaline,CC_upper_airway_obstruction
58,Apnoea requiring intervention,CC_apnoea
59,Acute severe asthma requiring intravenous bronchodilator therapy or continuous nebuliser,CC_asthma_acute_severe
60,Arterial line monitoring,CC_art_monitoring
61,Cardiac pacing via an external box (pacing wires or external pads or oesophageal pacing),CC_pacing_external
62,Central venous pressure monitoring,CC_CVP_monitoring
63,Bolus intravenous fluids (> 80 ml/kg/day) in addition to maintenance intravenous fluids,CC_IVF_bolus
64,Cardio-pulmonary resuscitation (CPR),CC_CPR
65,Extracorporeal membrane oxygenation (ECMO) or Ventricular Assist Device (VAD) or aortic balloon pump,CC_MCS
66,Haemodialysis (acute patients only i.e. excluding chronic),CC_haemodialysis
67,Plasma filtration or Plasma exchange,CC_plasma_filtration_exchange
68,ICP-intracranial pressure monitoring,CC_ICP_monitoring
69,Intraventricular catheter or external ventricular drain,CC_ventricular_drain_cath
70,Diabetic ketoacidosis (DKA) requiring continuous infusion of insulin,CC_DKA
71,Intravenous infusion of thrombolytic agent (limited to tissue plasminogen activator [tPA] and streptokinase),CC_thrombolysis
72,Extracorporeal liver support using Molecular Absorbent Liver Recirculating System (MARS),CC_MARS
73,Continuous pulse oximetry,CC_SpO2_monitoring
74,patient nursed in single occupancy cubicle,CC_side_room
80,Heated Humidified High Flow Therapy (HHHFT) (patient receiving HHHFT),CC_HHHFT
81,Presence of an umbilical venous line,CC_UVC
82,Continuous infusion of insulin (patient receiving a continuous infusion of insulin),CC_VRII
83,Therapeutic hypothermia (patient receiving therapeutic hypothermia),CC_therapeutic_hypothermia
84,patient has a Replogle tube in situ,CC_replogle
85,patient has an epidural catheter in situ,CC_epidural
86,patient has an abdominal silo,CC_abdo_silo
87,Administration of intravenous (IV) blood products,CC_blood_products
88,patient has a central venous or long line (Peripherally Inserted Central Catheter line) in situ,CC_PICC
89,patient has an indwelling urinary or suprapubic catheter in situ,CC_catheter
90,patient has a trans-anastomotic tube in situ following oesophageal atresia repair,CC_transanastomotic_tube
91,patient has confirmed clinical seizure(s) today and/or continuous cerebral function monitoring (CFM),CC_seizure_or_CFM
92,patient has a ventricular tap via needle or reservoir today,CC_ventricular_tap
93,patient has a stoma,CC_stoma
94,patient has arrhythmia requiring intravenous anti-arrhythmic therapy,CC_arrhythmia
95,patient has reduced conscious level (Glasgow Coma Score 12 or below) and hourly (or more frequent) Glasgow Coma Score monitoring,CC_GCS_under12_monitoring
96,Intravenous infusion of sedative agent (patient  receiving continuous intravenous infusion of sedative agent),CC_IV_sedative
97,patient has status epilepticus requiring treatment with continuous intravenous infusion,CC_status_epilepticus
99,"No Defined Critical Care Activity (patient is not receiving any of the critical care interventions listed above (Excluding code 21). For example, patient is on the Intensive Care Unit ready for discharge and is receiving normal care. This is the default code.",CC_no_defined_cc_activity
"""

# COMMAND ----------

import io

import pandas as pd

df = pd.read_csv(io.StringIO(csv))
df = spark.createDataFrame(df)
df.createOrReplaceTempView("ccu029_lkp_ccactcode")
# drop_table("ccu029_lkp_ccactcode")
create_table("ccu029_lkp_ccactcode")
display(df)
