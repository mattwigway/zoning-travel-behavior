# Predict congestion for all tract pairs, using multiprocessing for speed

import pandas as pd
import numpy as np
import sklearn.ensemble
import multiprocessing
import tqdm
import datetime
import openmatrix as omx
import geopandas as gp
import joblib
import os
from itertools import product
from glob import glob


MULTIPROCESS = False

# chunk size is in tract pairs, not in 
CHUNK_SIZE = 1_000 

# Initialization function for worker
def initialize_worker ():
    # None of this stuff should be huge. While it may be shareable, better to just have process-local copies
    global tract_centroids, rf, car_tt, netdist, colnames
    tract_centroids = gp.read_file('tract_centroids_density.json')
    # divide by zero issues
    tract_centroids.loc[tract_centroids.aland < 1e-5, ['pop_dens_sqkm', 'job_dens_sqkm']] = 0

    rf, colnames = joblib.load('../data/skim_rf.joblib')

    skims = omx.open_file('../la_abm/data/skims.omx')
    skim_idx = pd.read_parquet('../la_abm/data/skim_tracts.parquet')

    car_tt = pd.DataFrame(np.array(skims['car_freeflow']), columns=skim_idx.geoid, index=skim_idx.geoid).stack().rename('car_freeflow_tt')
    car_tt.index = car_tt.index.rename(['from_geoid', 'to_geoid'])

    netdist = pd.DataFrame(np.array(skims['car_distance_km']), columns=skim_idx.geoid, index=skim_idx.geoid).stack().rename('car_net_dist')
    netdist.index = netdist.index.rename(['from_geoid', 'to_geoid'])

    skims.close()

# Actual worker function (called with a chunk of along_route.csv)
def worker (filename):
    chunk = pd.read_parquet(filename)
    chunk = chunk.rename(columns='along_route_{}'.format).reset_index()

    # merge in origin and destination characteristics
    chunk = chunk.merge(
        tract_centroids.drop(columns=['aland', 'total_pop', 'NAME', 'state', 'county', 'tract', 'tract_geoid', 'total_jobs', 'geometry'])\
            .rename(columns='from_{}'.format),
        left_on='from_geoid',
        right_on='from_GEOID',
        how='left',
        validate='m:1'
    )

    chunk = chunk.merge(
        tract_centroids.drop(columns=['aland', 'total_pop', 'NAME', 'state', 'county', 'tract', 'tract_geoid', 'total_jobs', 'geometry'])\
            .rename(columns='to_{}'.format),
        left_on='to_geoid',
        right_on='to_GEOID',
        how='left',
        validate='m:1'
    )


    # merge in travel times
    chunk = chunk.set_index(['from_geoid', 'to_geoid'])

    chunk['car_freeflow_tt'] = car_tt.reindex(chunk.index)
    chunk['car_net_dist'] = netdist.reindex(chunk.index)

    # make sure nothing unexpected is null
    assert not chunk.to_job_dens_sqkm.isnull().any(), 'some to job densities null'
    assert not chunk.from_job_dens_sqkm.isnull().any(), 'some from job densities null'
    # okay for some bands to be null, no tracts in band
    assert not chunk.car_freeflow_tt.isnull().any(), 'some freeflow times null'
    assert not chunk.car_net_dist.isnull().any(), 'some distances null'

    # fill expected nulls
    chunk = chunk.fillna(-1)

    # broadcast to one record per hour
    n_pairs = len(chunk)
    chunk = chunk.iloc[np.repeat(np.arange(n_pairs), 24)].copy()
    chunk['hour'] = np.tile(np.arange(24), n_pairs)

    # extract estimation data
    est_data = chunk[colnames]

    # predict!
    chunk['pred_congestion_ratio'] = rf.predict(est_data)

    # return
    return chunk.reset_index().set_index(['from_geoid', 'to_geoid', 'hour']).pred_congestion_ratio

if __name__ == '__main__':
    tract_centroids = gp.read_file('tract_centroids_density.json')
    # divide by zero issues
    tract_centroids.loc[tract_centroids.aland < 1e-5, ['pop_dens_sqkm', 'job_dens_sqkm']] = 0

    if os.path.exists('congestion_cache/complete.mark'):
       print('using cached inputs')
    else: 
        if not os.path.exists('congestion_cache'):
            os.mkdir('congestion_cache')

        print('reading and reshaping inputs')

        # use dask for the reshaping bit to save memory
        along_route = pd.read_csv('../data/along_route.csv', dtype={'from_geoid': str, 'to_geoid': str})
        along_route_back = along_route.rename(columns={'from_geoid': 'to_geoid', 'to_geoid': 'from_geoid'})
        along_route = pd.concat([along_route, along_route_back], ignore_index=True)
        del along_route_back

        along_route = along_route.set_index(['from_geoid', 'to_geoid', 'band']).unstack().fillna(-1)
        along_route.columns = [f'{col}_{band[1]}_{band[4]}' for col, band in along_route.columns]

        # make sure every tract pair is included even if there were no tracts on the way
        along_route = along_route.reindex(list(product(tract_centroids.GEOID, tract_centroids.GEOID)), fill_value=-1)

        print('caching reshaped inputs')
        for i, chunk_start in enumerate(tqdm.trange(0, len(along_route), CHUNK_SIZE)):
            chunk_end = min(chunk_start + CHUNK_SIZE, len(along_route))
            along_route.iloc[chunk_start:chunk_end].to_parquet(f'congestion_cache/chunk_{i}.parquet')
        
        with open('congestion_cache/complete.mark', 'w') as markfile:
            markfile.write('Generation completed: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))        
        
    results = []

    # generator expression, don't materialize all at once
    files = glob('congestion_cache/chunk_*.parquet')

    if MULTIPROCESS:
        pool = multiprocessing.Pool(multiprocessing.cpu_count(), initializer=initialize_worker)

        total_length = int(len(tract_centroids) * len(tract_centroids))

        print(f'Parallelizing {total_length:,d} tasks over {multiprocessing.cpu_count()} processes in chunks of {CHUNK_SIZE}')

        iterable = pool.imap_unordered(worker, files)
    else:
        print('Parallelization not requested')
        initialize_worker()  # load needed globals in main process
        iterable = map(worker, files)

    for result in tqdm.tqdm(iterable, total=len(files)):
        results.append(result)

    if not MULTIPROCESS:
        # free RAM
        del tract_centroids, rf, car_tt, netdist, colnames

    final_result = pd.DataFrame({'congested_tt_ratio': pd.concat(results)})
    final_result.to_parquet('predicted_congestion_ratio.parquet')



