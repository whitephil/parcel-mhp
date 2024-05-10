import os
import requests
import geopandas as gpd
import warnings
import time

start = time.time()

warnings.filterwarnings('ignore')

countiesDir = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties'

co_counties00 = gpd.read_file(requests.get('https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2000/tl_2010_08_county00.zip').content)
co_counties10 = gpd.read_file(requests.get('https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_08_county10.zip').content)

# change to generate these lists from the parcel data folders so I don't end up with unnessecary folders and data:
fips00 = co_counties00['CNTYIDFP00'].to_list()
fips10 = co_counties10['GEOID10'].to_list()

url00 = 'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2000/'
url10 = 'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/'

for fips in fips00:
    if os.path.exists(os.path.join(countiesDir,fips)) == False:
        os.mkdir(os.path.join(countiesDir,fips))
    blocks00 = gpd.read_file(requests.get(url00+f'tl_2010_{fips}_tabblock00.zip').content)
    blocks00.rename(columns={'BLKIDFP00':'GEOID00'}, inplace=True)
    blocks00.to_crs(crs='ESRI:102005', inplace=True)
    blocks00.to_file(os.path.join(countiesDir,fips,f'{fips}_blocks.gpkg'), layer=f'{fips}_blocks00', driver='GPKG')
    


for fips in fips10:
    if os.path.exists(os.path.join(countiesDir,fips)) == False:
        os.mkdir(os.path.join(countiesDir,fips))
    blocks10 = gpd.read_file(requests.get(url10+f'tl_2010_{fips}_tabblock10.zip').content)
    blocks10.to_crs(crs='ESRI:102005', inplace=True)
    blocks10.to_file(os.path.join(countiesDir,fips,f'{fips}_blocks.gpkg'), layer=f'{fips}_blocks10', driver='GPKG')


print('Time:',time.time()-start)
