# Databricks notebook source
# This is a Python implementation of the lms2z function in the R package `sitar`, it is mainly written in numpy to remain as widely applicable as possible
# See the original R code here https://github.com/statist7/sitar/blob/master/R/LMS2z.R

# COMMAND ----------

# MAGIC %run ./uk90

# COMMAND ----------

import io

import numpy as np
import pandas as pd

# COMMAND ----------
from scipy.interpolate import CubicSpline
from tqdm import tqdm

uk90 = pd.read_csv(io.StringIO(uk90_raw))

# COMMAND ----------


def zLMS(val, L, M, S):
    L = 1e-7 if L == 0 else L
    return ((val / M) ** L - 1) / L / S


def cLMS(z, L, M, S):
    L = 1e-7 if L == 0 else L
    return M * (1 + L * S * z) ** (1 / L)


# COMMAND ----------


def LMS2z(x, y, sex, measure, toz=True):
    ref = uk90

    xy = pd.DataFrame({"x": x, "sex": sex})
    x = np.array(xy.x)
    sex = np.array(xy.sex)

    LMS = ["L", "M", "S"]
    LMSnames = [f"{lms}.{measure}" for lms in LMS]
    x[(x < min(ref.years)) | (x > max(ref.years))] = np.NaN

    for ix in [1, 2]:
        sexvar = sex == ix
        if any(sexvar):
            refx = ref[ref.sex == ix][["years"] + LMSnames].drop_duplicates().dropna()
            nref = np.where((refx.years - refx.years.shift(1)).dropna() == 0)[0]
            nref = np.append(nref, len(refx) - 1)

            end = -1
            for i in range(len(nref)):
                start = end + 1
                end = nref[i]

                refrange = (x[sexvar] >= refx.years.iloc[start]) & (x[sexvar] <= refx.years.iloc[end])
                refrange[refrange != refrange] = False
                refrange = np.where(sexvar)[0][refrange]

                if len(refrange) > 0:
                    refx_subset = refx.iloc[start : (end + 1),]
                    for LMSshort, LMSname in tqdm(zip(LMS, LMSnames)):
                        xy.loc[refrange, LMSshort] = CubicSpline(refx_subset["years"], refx_subset[LMSname])(
                            x[refrange]
                        )

    if toz:
        cz = [None if not val else zLMS(float(val), L, M, S) for (val, (L, M, S)) in tqdm(zip(y, xy[LMS].values))]
    else:
        cz = [None if not z else cLMS(float(z), L, M, S) for (z, (L, M, S)) in tqdm(zip(y, xy[LMS].values))]

    print("Calculated Z-Scores, writing to Spark...")
    return cz


# COMMAND ----------

# measure = "bmi"

# xy = pd.DataFrame({"x" : x, "sex" : sex, "y" : y})
# x = np.array(xy.x)
# sex = np.array(xy.sex)
# LMS = ['L', 'M', 'S']
# LMSnames = [f"{lms}.{measure}" for lms in LMS]
# x[(x < min(uk90.years)) | (x > max(uk90.years))] = np.NaN

# COMMAND ----------

# ix = 2
# sexvar = sex == ix

# ref = uk90
# refx = ref[ref.sex == ix][["years"] + LMSnames].drop_duplicates().dropna()
# nref = np.where((refx.years - refx.years.shift(1)).dropna() == 0)[0]
# nref = np.append(nref, len(refx) - 1)

# end = -1

# i = 0
# start = end + 1
# end = nref[i]

# refrange = (x[sexvar] >= refx.years.iloc[start]) & (x[sexvar] <= refx.years.iloc[end])
# refrange[refrange != refrange] = False
# refrange = np.where(sexvar)[0][refrange]

# COMMAND ----------

# refx_subset = refx.iloc[start:(end+1), ]
# for (LMSshort, LMSname) in zip(LMS, LMSnames):
#   xy.loc[refrange, LMSshort] = CubicSpline(refx_subset["years"], refx_subset[LMSname])(x[refrange])

# COMMAND ----------

# cz = [None if not val else zLMS(float(val), L, M, S) for (val, (L, M, S)) in zip(y, xy[LMS].values)]
# print(cz)
