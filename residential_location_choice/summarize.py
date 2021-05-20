import dask.dataframe as dd
import dask.distributed
import re
from glob import glob
import pandas as pd
import numpy as np
import zipfile
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger()
file_handler = logging.FileHandler("summarize.log", mode="w", encoding="UTF-8")
file_handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
LOG.addHandler(file_handler)

cluster = dask.distributed.LocalCluster(processes=False, n_workers=1, threads_per_worker=1)
client = dask.distributed.Client(cluster)

def weighted_percentile(vals, percentiles, weights):
    if len(vals) != len(weights):
        raise ArgumentError("values and weights arrays are not same length!")

    nas = pd.isnull(vals) | pd.isnull(weights)

    nnas = np.sum(nas)
    if nnas > 0:
        warn(f"found {nnas} NAs in data, dropping them")

    vals = vals[~nas]
    weights = weights[~nas]

    weights = weights / np.sum(weights)
    sortIdx = np.argsort(vals)
    vals = vals.iloc[sortIdx]
    weights = weights.iloc[sortIdx]

    cumWeights = np.cumsum(weights)
    if not isinstance(percentiles, np.ndarray):
        percentiles = np.array(percentiles)
    percentiles = percentiles / 100

    # center weights, i.e. put the point value halfway through the weight
    # https://github.com/nudomarinero/wquantiles/blob/master/wquantiles.py
    centeredCumWeights = cumWeights - 0.5 * weights
    return np.interp(percentiles, centeredCumWeights, vals)

refmt = re.compile(r'(?:.*simulation_)(?P<scenario>.*?)(?:(?:-2021.*.parquet.zip$)|(?:.parquet.zip$))')
def extract_sname (fname):
    if (m := refmt.match(fname)):
        return m.group('scenario')
    else:
        return fname
    
hh = dd.read_parquet('~/npv_sorting/full_hh.parquet')
print('read hh')

def median_inc_for_puma (ddf):
    # materialize probabilities for puma
    return weighted_percentile(ddf.hhincome, 50, ddf.weighted_prob)

def compute_median_income (zfn):
    with zipfile.ZipFile(zfn) as zin:
        zin.extract('probabilities.parquet.gz')

    probs = dd.read_parquet('probabilities.parquet.gz')

    probs = probs.merge(hh[['hhwt', 'hhincome']], left_on='hh', right_index=True, how='left')
    
    probs['puma'] = probs.housing.str.slice(0, 5)

    probs['weighted_prob'] = probs.probability * probs.hhwt

    veh_ownership_levels = probs.groupby('uneq_choice').weighted_prob.sum().compute()

    median_incomes = (
        probs.groupby(['puma', 'hh'])
            # make the median faster by presumming within households
            .aggregate({'weighted_prob': 'sum', 'hhincome': 'max'})
            .reset_index()
            .groupby(['puma'])
            .apply(median_inc_for_puma)
            .compute()
    )
    
    return median_incomes, veh_ownership_levels

med_inc = {}
veh_own = {}

for f in glob('/home/mwconway/npv_sorting/simulation*.parquet.zip'):
    LOG.info(f)
    med_inc[f], veh_own[f] = compute_median_income(f)
    LOG.info(med_inc[f].head())

veh_own = pd.DataFrame(veh_own).transpose()
veh_own.index = list(map(extract_sname, veh_own.index))
veh_own.to_parquet('~/npv_sorting/veh_own.parquet')

meddf = pd.DataFrame(med_inc).transpose()
meddf.index = list(map(extract_sname, meddf.index))
meddf.to_parquet('~/npv_sorting/median_income.parquet')
