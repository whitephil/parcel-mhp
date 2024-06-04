import os
import requests
import pandas as pd
import geopandas as gpd
import sys
import warnings
import time
import csv
import errno

start = time.time()

gpd.options.io_engine = "pyogrio"

warnings.filterwarnings('ignore')

fips = sys.argv[1].strip()

data_dir = r'/scratch/alpine/phwh9568'
#inv_path = os.path.join(data_dir,'inventory.csv')
proj_dir = r'/projects/phwh9568/mh_v1'
res_dir = os.path.join(proj_dir,'results')
state_dir = os.path.join(data_dir,f'State_{fips[0:2]}')
#countiesDir = os.path.join(data_dir,'counties')

url = 'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/'

years = ['10','00']


#if os.path.exists(state_dir) == False:
#    try:
#        os.mkdir(state_dir)
#    except OSError as e:
#        if e.errno != errno.EEXIST:
#            pass

if os.path.exists(os.path.join(state_dir,fips)) == False:
    os.mkdir(os.path.join(state_dir,fips))


with open(os.path.join(res_dir,'exceptions_blocks.csv'),'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    gpkg = os.path.join(state_dir,fips,f'{fips}_blocks.gpkg')
    if os.path.exists(gpkg) == False:
        for year in years:
            query = url+f'20{year}/tl_2010_{fips}_tabblock{year}.zip'
            response = requests.get(query)
            if response.status_code != 200:
                writer.writerow([fips,year,'bad request'])
                continue
            blocks = gpd.read_file(response.content)
            if year == '00':
                blocks.rename(columns={'BLKIDFP00':'GEOID00'}, inplace=True)
            blocks.to_crs(crs='ESRI:102005', inplace=True)
            blocks.to_file(gpkg, layer=f'{fips}_blocks{year}', driver='GPKG')

print('Time:',time.time()-start)
