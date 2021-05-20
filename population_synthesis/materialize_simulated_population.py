#!/usr/bin/env python
# coding: utf-8

# # Materialize simulated population
# #
# After households have been drawn using `popsyn.jl`, this file moves from the list of household IDs to household and person files in the
# format ActivitySim/MTC TM1 expect (see https://activitysim.github.io/activitysim/abmexample.html#inputs)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from argparse import ArgumentParser
import os

# ActivitySim uses values in 2000 dollars - this is CPI inflation from Jan 2000 to Jan 2017
# (https://www.bls.gov/data/inflation_calculator.htm)
INFLATION_2000_2017 = 1.44

parser = ArgumentParser()
parser.add_argument('hh_list', help="Household list (Parquet format) from popsyn.jl")
parser.add_argument('out_dir', help="Output directory for materialized population")
args = parser.parse_args()

pop = pd.read_parquet(args.hh_list)

# read hh info
# TODO add values needed for person file
ipums_pers = pd.read_sql(
    """
    SELECT serial, pernum, numprec, hhincome, hhtype, empstat, age, sex, vehicles, hhwt, uhrswork, school, gradeatt 
    FROM ipums WHERE gq NOT IN ('Group quarters--Institutions', 'Other group quarters')
    """,
    "sqlite:////Volumes/Pheasant Ridge/IPUMS/scag_sorting_5yr_abm/scag_sorting_5yr_abm.db",
)

ipums_hh = ipums_pers.groupby("serial").first()
ipums_hh["num_workers"] = (
    ipums_pers[ipums_pers.empstat == "Employed"]
    .groupby("serial")
    .size()
    .reindex(ipums_hh.index, fill_value=0)
)

# derive ActivitySim named and coded variables
# all costs in activitysim are in 2000 dollars
ipums_hh["income"] = ipums_hh.hhincome / INFLATION_2000_2017
assert not ipums_hh.income.isnull().any()

ipums_hh["HHT"] = ipums_hh.hhtype.map(
    {
        "HHTYPE could not be determined": 0,
        "Married-couple family household": 1,
        "Male householder, no wife present": 2,
        "Female householder, no husband present": 3,
        "Male householder, living alone": 4,
        "Male householder, not living alone": 5,
        "Female householder, living alone": 6,
        "Female householder, not living alone": 7,
    }
).astype("int64")
assert not ipums_hh.HHT.isnull().any()

ipums_hh["hhsize"] = ipums_hh.numprec.replace({"1 person record": "1"}).astype("int64")


# ### Merge the IPUMS characteristics into the synthetic population

hh = pop.merge(
    ipums_hh[["income", "HHT", "hhsize", "num_workers"]],
    left_on="hh",
    right_index=True,
    how="left",
    validate="m:1",
)


hh["household_id"] = np.arange(len(hh))


hh.head()


# add TAZ variable based on tract_geoid
tract_mapping = pd.read_parquet("../la_abm/data/skim_tracts.parquet")


hh = hh.merge(
    tract_mapping.rename(columns={"idx": "TAZ"}),
    left_on="tract_geoid",
    right_on="geoid",
    how="left",
    validate="m:1",
)


assert not hh.TAZ.isnull().any()
assert not hh.income.isnull().any()


# ### Compute vehicle ownership
#
# We only predicted up to 3+, but the model needs up to 6+. Just disaggregate the 3+ category randomly, based on proportion in the base IPUMS.


ipums_hh["vehicles_int"] = ipums_hh.vehicles.replace(
    {
        "1 available": "1",
        "6 (6+, 2000, ACS and PRCS)": "6",
        "No vehicles available": "0",
    }
).astype("int64")


veh_choice_probs = (
    ipums_hh.loc[ipums_hh.vehicles_int >= 3].groupby("vehicles_int").hhwt.sum()
)
veh_choice_probs /= veh_choice_probs.sum()

# pop.hh.sum() is not a meaningful number, but forces same results with same input
rng = np.random.default_rng(pop.hh.sum())
hh["auto_ownership"] = hh.uneq_choice
hh.loc[hh.uneq_choice == 3, "auto_ownership"] = rng.choice(
    veh_choice_probs.index,
    (hh.uneq_choice == 3).sum(),
    replace=True,
    p=veh_choice_probs,
)


# check our work - simulated distr. of >=3 veh hhs should be quite close to what we calculated from the
# IPUMS (by construction)
# recall that hh is unweighted
sim_veh_choice_probs = hh[hh.auto_ownership >= 3].groupby("auto_ownership").size()
sim_veh_choice_probs /= sim_veh_choice_probs.sum()
print(pd.DataFrame({"expected": veh_choice_probs, "simulated": sim_veh_choice_probs}))


# ### Write out households file


hh[
    "sample_rate"
] = 1  # not sure what this does, but it's 1 for every household in the TM1 sample
out_hh = hh[
    [
        "household_id",
        "TAZ",
        "income",
        "hhsize",
        "HHT",
        "auto_ownership",
        "num_workers",
        "sample_rate",
        "rent"
    ]
]
assert not out_hh.isnull().any().any()


out_hh.to_csv(os.path.join(args.out_dir, "households.csv"), index=False)


# ## Create persons file


# merge from hh file so we have the generated household id
pers = hh.merge(ipums_pers, left_on="hh", right_on="serial", how="left", validate="m:m")


# check the average household size
len(pers) / len(pop)


out_hh.hhsize.mean()


np.average(ipums_hh.hhsize, weights=ipums_hh.hhwt)


pers["person_id"] = np.arange(len(pers))


pers["age"] = pers.age.replace(
    {"Less than 1 year old": "0", "90 (90+ in 1980 and 1990)": "90"}
).astype("int64")
pers.age.describe()


pers["PNUM"] = pers.pernum


pers["sex"] = pers.sex.replace({"Male": 1, "Female": 2}).astype("int64")


pers.empstat


pers.uhrswork.unique()


# I don't know why I have to convert twice here but doesn't work otherwise
pers["ihrswork"] = (
    pers.uhrswork.replace({"99 (Topcode)": "99", "N/A": "-1"})
    .astype("int64")
    .astype("Int64")
)
pers.loc[pers.ihrswork < 0, "ihrswork"] = np.nan
assert not pers.loc[pers.empstat == "Employed", "ihrswork"].isnull().any()


pers["pemploy"] = -1
pers.loc[
    (pers.empstat == "Employed") & (pers.ihrswork >= 30), "pemploy"
] = 1  # full time worker
pers.loc[
    (pers.empstat == "Employed") & (pers.ihrswork < 30), "pemploy"
] = 2  # part time worker
pers.loc[
    pers.empstat.isin(["Unemployed", "Not in labor force"]), "pemploy"
] = 3  # not in labor force
pers.loc[pers.age < 16, "pemploy"] = 4  # under 16
assert not (pers.pemploy < 0).any()


pers["pstudent"] = pers.gradeatt.map(
    {
        "N/A": 3,  # not student
        "Grade 9 to grade 12": 1,  # Pre-K - 12
        "Grade 1 to grade 4": 1,
        "Nursery school/preschool": 1,
        "College undergraduate": 2,  # univ/professional school
        "Graduate or professional school": 2,
        "Grade 5 to grade 8": 1,
        "Kindergarten": 1,
    }
)
# very young children have gradeatt = N/A if they are not in preschool
assert (
    pers.loc[(pers.pstudent == 3) & (pers.age > 2), "school"] == "No, not in school"
).all()


pers["ptype"] = -1

# full time
pers.loc[(pers.pemploy == 1) & (pers.age >= 16), 'ptype'] = 1 # full time

# part time
pers.loc[(pers.pemploy == 2) & (pers.pstudent == 1) & (pers.age >= 16) & (pers.age <= 19), 'ptype'] = 6 # driving age student non college
pers.loc[(pers.pemploy == 2) & (pers.pstudent == 1) & (pers.age >= 20), 'ptype'] = 3  # college student (even though they're not...)
pers.loc[(pers.pemploy == 2) & (pers.pstudent == 2) & (pers.age >= 16), 'ptype'] = 3
pers.loc[(pers.pemploy == 2) & (pers.pstudent == 3) & (pers.age >= 16), 'ptype'] = 2 # part time worker

# not working
pers.loc[(pers.pemploy == 3) & (pers.pstudent == 1) & (pers.age >= 16) & (pers.age <= 19), 'ptype'] = 6
pers.loc[(pers.pemploy == 3) & (pers.pstudent == 1) & (pers.age >= 20), 'ptype'] = 3  # once again, called college students even if they're not
pers.loc[(pers.pemploy == 3) & (pers.pstudent == 2) & (pers.age >= 16), 'ptype'] = 3
pers.loc[(pers.pemploy == 3) & (pers.pstudent == 3) & (pers.age >= 16) & (pers.age <= 64), 'ptype'] = 4
pers.loc[(pers.pemploy == 3) & (pers.pstudent == 3) & (pers.age >= 65), 'ptype'] = 5  # retired

# under 16
pers.loc[(pers.pemploy == 4) & (pers.age <= 5), 'ptype'] = 8
pers.loc[(pers.pemploy == 4) & (pers.age >= 6) & (pers.age <= 15), 'ptype'] = 7

assert not (pers.ptype == -1).any()

# pers.loc[(pers.pemploy == 1) & (pers.age >= 18), "ptype"] = 1  # Full time
# pers.loc[(pers.pemploy == 2) & (pers.age >= 18), "ptype"] = 2  # Part time
# # will you still need me, will you still feed me, when I'm sixty-four?
# pers.loc[
#     (pers.pemploy == 3) & (pers.age >= 18) & (pers.age <= 64), "ptype"
# ] = 4  # non-working adult
# pers.loc[(pers.pemploy == 3) & (pers.age >= 65), "ptype"] = 5  # retired

# # student status trumps worker status
# pers.loc[
#     (pers.pstudent == 2) & (pers.age >= 18), "ptype"
# ] = 3  # college student (overwrites work status)
# pers.loc[
#     (pers.pstudent == 1) & (pers.age >= 16), "ptype"
# ] = 6  # driving age student
# pers.loc[
#     (pers.pstudent == 1) & (pers.age >= 6) & (pers.age < 16), "ptype"
# ] = 7  # non-driving student
# pers.loc[
#     pers.age <= 5, "ptype"
# ] = 8  # preschool - assuming all under 5 are preschool - this appears to be what was done in SF sample

# # undocumented things to get everyone to have a ptype - matches what was done in SF sample
# # 16 and 17 year old college students were classified as 1 if they worked full time, 3 otherwise
# pers.loc[
#     (pers.ptype == -1) & pers.age.isin([16, 17]) & (pers.pemploy == 1), "ptype"
# ] = 1
# pers.loc[
#     (pers.ptype == -1) & pers.age.isin([16, 17]) & (pers.pemploy != 1), "ptype"
# ] = 3

# pers.loc[
#     (pers.ptype == -1)
#     & pers.age.isin(np.arange(6, 16))
#     & (pers.pemploy == 4)
#     & (pers.pstudent != 1),
#     "ptype",
# ] = 7

# non driving age students should not be employed in ActivitySim-world
#pers.loc[pers.ptype == 7, 'pemploy'] = 4

# if these do not hold, then the trip_purpose module will crash
assert not np.any((pers.ptype == 2) & (pers.pstudent != 3))
assert not np.any((pers.ptype == 7) & (pers.pemploy != 4))

assert not (pers.ptype == -1).any()


out_pers = pers[
    ["person_id", "household_id", "age", "PNUM", "sex", "pemploy", "pstudent", "ptype"]
]
assert not out_pers.isnull().any().any()
out_pers.to_csv(os.path.join(args.out_dir, "persons.csv"), index=False)


len(out_hh)


len(out_pers)


assert not out_hh.household_id.duplicated().any()
