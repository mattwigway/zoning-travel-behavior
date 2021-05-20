import pandas as pd
import numpy as np
import geopandas as gp
from sys import argv
import os
import subprocess

n, e, s, w = map(int, argv[1:5])
output_dir = argv[5]

print(f'{(n, e, s, w)=}')

for lat in range(s, n + 1):
    for lon in range(w, e + 1):
        latdir = 'n' if lat >= 0 else 's'
        londir = 'e' if lon >= 0 else 'w'
        fname = f'{latdir}{abs(lat):02d}{londir}{abs(lon):03d}'
        print(fname)

        url = f'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/{fname}/USGS_13_{fname}.tif'

        print(url)

        outf = os.path.join(output_dir, f'{fname}.tif')

        subprocess.run(['curl', '--output', outf, url])
