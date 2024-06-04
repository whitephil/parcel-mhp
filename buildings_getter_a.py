import geopandas as gpd
import pandas as pd
import requests
import os
import warnings
import sys

sfips = sys.argv[1].strip()
print(sfips)
warnings.filterwarnings("ignore")
gpd.options.io_engine = "pyogrio"

data_dir = r'/scratch/alpine/phwh9568'
countiesPath = os.path.join(data_dir,'tl_2010_09_county10.gpkg')
inventoryPath = os.path.join(data_dir,'inventory.csv')
out_dir = os.path.join(data_dir,f'State_{sfips}')

if os.path.exists(out_dir) == False:
    os.mkdir(out_dir)

url = 'https://usbuildingdata.blob.core.windows.net/usbuildings-v2/'

inventory = pd.read_csv(inventoryPath, dtype={'FIPS':str})
inventory = inventory.loc[inventory['Parcels_Present']==True]
inventory = inventory['FIPS'].to_list()
print(inventory)

counties = gpd.read_file(countiesPath, dtype={'STATEFP':str, 'COUNTYFP':str, 'GEOID':str})
stateFips = pd.read_csv(os.path.join(data_dir,'stateFips.csv'), dtype={'STATEFP':str, 'STATENS':str})
name = stateFips.loc[stateFips['STATEFP']==sfips]['STATE_NAME'].values[0]

stateUrl = url+name.replace(' ','')+'.geojson.zip'
stateBuildings = gpd.read_file(requests.get(stateUrl).content)
stateBuildings['FINDEX'] = stateBuildings.index
stateCounties = counties.loc[counties['STATEFP'] == sfips]
print('len state counties:', len(stateCounties))
stateGEOIDs = stateCounties['GEOID'].to_list()
print('stateGEOIDs:', stateGEOIDs)
stateWithins = gpd.sjoin(stateBuildings, stateCounties, how='left', op='within')
stateWithins.reset_index(inplace=True)
stateWithins.drop(['index_right'], axis = 1, inplace=True)
print('len stateWithins:', len(stateWithins))
nulls = stateWithins.loc[stateWithins['GEOID'].isna()]
keepCols = stateBuildings.columns.to_list()
keepCols.append('GEOID')
stateIntersects = gpd.sjoin(nulls, stateCounties, how='left', op='intersects')
stateIntersects.rename({'GEOID_right':'GEOID'}, axis='columns', inplace=True)
drops = [c for c in stateIntersects.columns if c not in keepCols]
stateIntersects.drop(drops, axis=1, inplace=True)
stateIntersects.dropna(subset=['GEOID'], inplace=True)
print('len stateIntersects:', len(stateIntersects))
drops = [c for c in stateWithins.columns if c not in keepCols]
stateWithins.drop(drops, axis=1, inplace=True)
stateWithins.dropna(subset=['GEOID'], inplace=True)
for fips in stateGEOIDs:
    if fips in inventory:
        print(fips)
        fileDir = os.path.join(out_dir,fips)
        if os.path.exists(fileDir) == False:
            os.mkdir(fileDir)
        layerName = fips+'_Buildings'
        if os.path.exists(os.path.join(fileDir,layerName+'.gpkg')) == False:
            countyWithins = stateWithins.loc[stateWithins['GEOID'] == fips]
            print('len county withins:', len(countyWithins))
            countyIntersects = stateIntersects.loc[stateIntersects['GEOID'] == fips]
            print('len county intersects:', len(countyIntersects))
            countyBuildings = pd.concat([countyWithins, countyIntersects], ignore_index=True)
            print('len countyBuildings:', len(countyBuildings))
            countyBuildings.rename({'GEOID':'fips'}, axis='columns', inplace = True)
            countyBuildings.to_crs(crs='ESRI:102005')
            countyBuildings.to_file(os.path.join(fileDir,layerName+'.gpkg'), driver='GPKG')