# This implements the polygon-in-polygon tests that are used to determine what buildings are feasible on each parcel

import numpy as np
import geopandas as gp
import rasterio.features
import rasterio.transform
import shapely.affinity
import math
import numba
import tqdm
import csv
import pandas as pd
import multiprocessing

DIMENSIONS = (
    (12, 10),
    (16, 11),
    (21, 10)
)

DIM_NAMES = (
    'fit_sfh_duplex',
    'fit_fourplex',
    'fit_sixplex'
)


# numba really does help here, 32us -> ~1us.
@numba.jit(nopython=True)
def fits (mask, width, height):
    '''
    This function checks whether a rectangle of width x height can fit inside the rasterized polygon described by
    mask.
    '''
    for x in range(mask.shape[0]):
        for y in range(mask.shape[1]):
            if mask[x, y]:
                # note that these < should be <=, see note on page 34 of dissertation
                if ((x + width) < mask.shape[0] and (y + height) < mask.shape[1]
                        and np.all(mask[x:x + width, y:y + height])):
                    return True
                if ((x + height) < mask.shape[0] and (y + width) < mask.shape[1]
                        and np.all(mask[x:x + height, y:y + width])):
                    return True
    return False


def rect_fit (geom, dims):
    if geom is None or geom.is_empty:
        # short-circuit for null/empty geoms, rasterio.features.bounds chokes on empty geoms
        return np.array([False for d in dims])

    w, s, e, n = rasterio.features.bounds(geom)
    # force width/height to exact meters so pixels are whole meters
    w = math.floor(w)
    s = math.floor(s)
    n = math.ceil(n)
    e = math.ceil(e)
    assert (e - w) % 1 == 0
    assert (s - n) % 1 == 0
    width = int(round(e - w))
    height = int(round(n - s))
    xform = rasterio.transform.from_bounds(w, s, e, n, width, height)
    mask = rasterio.features.geometry_mask(geom, (height, width), xform, invert=True)
    return [fits(mask, w, h) for w, h in dims]


def rot_fit (geom, dims, rotations_deg=np.arange(0, 90, 15)):
    '''
    Check if it fits for all possible rotations, 0-90 degrees. Only need to rotate through 90 degrees because fit()
    checks for fit both horizontally and vertically, and because rectangles are symmetrical.
    '''

    dims = np.array(dims)
    out = np.array([False for dim in dims])

    for rot in rotations_deg:
        if rot == 0:
            rot_geom = geom
        else:
            rot_geom = shapely.affinity.rotate(geom, rot, use_radians=False)

        out[~out] |= rect_fit(rot_geom, dims[~out])

        if np.sum(~out) == 0:
            break

    return out


def queue_filler (task_queue):
    for chunk in gp.read_postgis('SELECT gid, apn, ST_Transform(geog::geometry, 26911) AS geom FROM diss.buildable_areas',
                                 'postgresql://matthewc@localhost:5432/matthewc',
                                 chunksize=5000):
        for row in chunk.itertuples(index=False):
            # namedtuples don't pickle nicely
            task_queue.put(tuple(row), block=True)  # block if queue is full


def queue_consumer (task_queue, result_queue):
    while True:
        gid, apn, geom = task_queue.get(block=True)
        res = rot_fit(geom, DIMENSIONS)
        result_queue.put((gid, apn, *res), block=True)


if __name__ == '__main__':
    total = pd.read_sql('SELECT count(*) FROM diss.buildable_areas', 'postgresql://matthewc@localhost:5432/matthewc').iloc[0, 0]
    multiprocessing.set_start_method('spawn')
    task_queue = multiprocessing.Queue(5000)
    result_queue = multiprocessing.Queue(5000)

    # start processes
    fill_process = multiprocessing.Process(target=queue_filler, args=(task_queue,))
    fill_process.start()

    compute_processes = [
        multiprocessing.Process(target=queue_consumer, args=(task_queue, result_queue))
        for i in range(multiprocessing.cpu_count())
    ]

    for p in compute_processes:
        p.start()

    # write output in main thread so we know when to stop
    with open('dim_fit.csv', 'w') as out:
        writer = csv.writer(out)
        writer.writerow(['gid', 'apn', *DIM_NAMES])
        with tqdm.tqdm(total=total) as pbar:
            count = 0
            while count < total:
                writer.writerow(result_queue.get(block=True))
                count += 1
                pbar.update(1)

    fill_process.terminate()
    for p in compute_processes:
        p.terminate()
