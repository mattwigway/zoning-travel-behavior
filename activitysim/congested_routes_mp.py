#!/usr/bin/env python

import pandas as pd
import numpy as np
import geopandas as gp
import rtree
import tqdm
import shapely
import multiprocessing
import csv

# break a linestring for use in indexing
def break_arc_to_bboxes (x0, y0, x1, y1, target_length):
    total_length = ((x0 - x1)**2 + (y0 - y1)**2)**0.5
    n_segs = int(total_length // target_length + 1)
    endpoints = np.linspace(0, 1, n_segs)
    
    xstart = x0 + (x1 - x0) * endpoints[:-1]
    xend = x0 + (x1 - x0) * endpoints[1:]
    ystart = y0 + (y1 - y0) * endpoints[:-1]
    yend = y0 + (y1 - y0) * endpoints[1:]
    
    l = np.minimum(xstart, xend)
    r = np.maximum(xstart, xend)
    b = np.minimum(ystart, yend)
    t = np.maximum(ystart, yend)
        
    return l, b, r, t

def get_tracts_for_arc (x0, y0, x1, y1, dmax=8):
    tracts = set()
    # break the linestring into pieces 1km long so that we don't 
    bboxes = break_arc_to_bboxes(x0, y0, x1, y1, 4000)
    m = dmax * 1000 # convert to meters
    for l, b, r, t in zip(*bboxes):  # left bot right top
        tracts.update(tract_idx.intersection((l - m, b - m, r + m, t + m)))
    
    return tracts

def initialize_worker ():
    # ugly but set these as global variables so they remain accessible - each mp worker has separate process
    # https://stackoverflow.com/questions/10117073
    global tract_centroids, tract_idx
    tract_centroids = gp.read_file('tract_centroids_density.json')

    # build a spatial index
    tract_idx = rtree.index.Index()
    for idx, x, y in zip(tract_centroids.index, tract_centroids.geometry.x, tract_centroids.geometry.y):
        tract_idx.insert(idx, (x, y, x, y))

def worker (task):
    fridx, toidx, frx, fry, tox, toy = task
    band_results = []

    candidate_tracts = tract_centroids.loc[get_tracts_for_arc(frx, fry, tox, toy, 8)]

    if len(candidate_tracts) > 0:
        ls = shapely.geometry.LineString(((frx, fry), (tox, toy)))
        distances = candidate_tracts.distance(ls)

        for low, high in [(0, 2), (2, 4), (4, 6), (6, 8)]:
            tracts_in_dist = candidate_tracts[(distances > ((low * 1000))) & (distances <= ((high * 1000)))]
            if len(tracts_in_dist) == 0:
                continue

            band_results.append((fridx, toidx, f'({low}, {high}]', 
                *np.percentile(tracts_in_dist.pop_dens_sqkm, [25, 50, 75, 95]),
                *np.percentile(tracts_in_dist.job_dens_sqkm, [25, 50, 75, 95]),
                ))

    return band_results

if __name__ == '__main__':
    tract_centroids = gp.read_file('tract_centroids_density.json')

    # generator comprehension, everything is lazy
    tasks = (
        (fridx, toidx, frx, fry, tox, toy)
        for fri, fridx, frx, fry in zip(range(len(tract_centroids.index)), tract_centroids.index, tract_centroids.geometry.x, tract_centroids.geometry.y)
        for toidx, tox, toy in zip(tract_centroids.index[fri + 1:], tract_centroids.geometry.iloc[fri + 1:].x, tract_centroids.geometry.iloc[fri + 1:].y)
    )

    pool = multiprocessing.Pool(multiprocessing.cpu_count(), initializer=initialize_worker)

    total_length = int(len(tract_centroids) * (len(tract_centroids) - 1) / 2)

    print(f'Parallelizing {total_length:,d} tasks over {multiprocessing.cpu_count()} processes')

    # flatten results
    with open('along_route.csv', 'w') as output:
        writer = csv.writer(output)
        writer.writerow(['fromidx', 'toidx', 'band', 'pop_dens_sqkm_25',
                    'pop_dens_sqkm_50',
                    'pop_dens_sqkm_75',
                    'pop_dens_sqkm_95',
                    'job_dens_sqkm_25',
                    'job_dens_sqkm_50',
                    'job_dens_sqkm_75',
                    'job_dens_sqkm_95',
                    'from_geoid',
                    'to_geoid'])

        for result in tqdm.tqdm(pool.imap_unordered(worker, tasks, 100), total=total_length):
            for row in result:
                row = (*row, tract_centroids.loc[row[0], 'GEOID'], tract_centroids.loc[row[1], 'GEOID'])
                writer.writerow(row)
