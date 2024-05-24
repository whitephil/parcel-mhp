import os
import requests
import pandas as pd
import geopandas as gpd
import sys
import warnings
import time

start = time.time()

gpd.options.io_engine = "pyogrio"

warnings.filterwarnings('ignore')

data_dir = r'F:\ParcelAtlas2023'
inv_path = os.path.join(data_dir,'inventory.csv')
countiesDir = os.path.join(data_dir,'counties')

url = 'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/'

years = ['00','10']

fips = sys.argv[1]

if os.path.exists(os.path.join(countiesDir,fips)) == False:
    os.mkdir(os.path.join(countiesDir,fips))

for year in years:        
    blocks = gpd.read_file(requests.get(url+f'20{year}/tl_2010_{fips}_tabblock{year}.zip').content)
    if year == '00':
        blocks.rename(columns={'BLKIDFP00':'GEOID00'}, inplace=True)
    blocks.to_crs(crs='ESRI:102005', inplace=True)
    blocks.to_file(os.path.join(countiesDir,fips,f'{fips}_blocks.gpkg'), layer=f'{fips}_blocks{year}', driver='GPKG')

print('Time:',time.time()-start)
