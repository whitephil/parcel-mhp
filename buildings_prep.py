import time
start = time.time()

import geopandas as gpd
import pandas as pd
import requests
import os
import warnings

gpd.options.io_engine = "pyogrio"

warnings.filterwarnings("ignore")
dataDir = r'C:\Users\phwh9568\Data\ParcelAtlas'
countiesPath = os.path.join(dataDir,'tl_2022_us_county.gpkg')
outDir = r'E:/'
url = 'https://usbuildingdata.blob.core.windows.net/usbuildings-v2/'

counties = gpd.read_file(countiesPath, dtype={'STATEFP':str, 'COUNTYFP':str, 'GEOID':str})
stateFips = pd.read_csv(os.path.join(dataDir,'stateFips.csv'), dtype={'STATEFP':str, 'STATENS':str})
stateFipsDict=dict(zip(stateFips['STATEFP'].to_list(), stateFips['STATE_NAME'].to_list()))

for k,v in stateFipsDict.items():
    if k == '44':
        stateUrl = url+v.replace(' ','')+'.geojson.zip'
        stateBuildings = gpd.read_file(requests.get(stateUrl).content)
        stateBuildings['FINDEX'] = stateBuildings.index
        stateCounties = counties.loc[counties['STATEFP'] == k]
        stateGEOIDs = stateCounties['GEOID'].to_list()
        stateWithins = gpd.sjoin(stateBuildings, stateCounties, how='left', op='within')
        stateWithins.reset_index(inplace=True)
        stateWithins.drop(['index_right'], axis = 1, inplace=True)
        nulls = stateWithins.loc[stateWithins['GEOID'].isna()]
        keepCols = stateBuildings.columns.to_list()
        keepCols.append('GEOID')
        stateIntersects = gpd.sjoin(nulls, stateCounties, how='left', op='intersects')
        stateIntersects.rename({'GEOID_right':'GEOID'}, axis='columns', inplace=True)
        drops = [c for c in stateIntersects.columns if c not in keepCols]
        stateIntersects.drop(drops, axis=1, inplace=True)
        stateIntersects.dropna(subset=['GEOID'], inplace=True)
        drops = [c for c in stateWithins.columns if c not in keepCols]
        stateWithins.drop(drops, axis=1, inplace=True)
        stateWithins.dropna(subset=['GEOID'], inplace=True)
        for fips in stateGEOIDs:
            countyWithins = stateWithins.loc[stateWithins['GEOID'] == fips]
            countyIntersects = stateIntersects.loc[stateIntersects['GEOID'] == fips]
            countyBuildings = pd.concat([countyWithins, countyIntersects], ignore_index=True)
            countyBuildings.rename({'GEOID':'fips'}, axis='columns', inplace = True)
            layerName = fips+'_Buildings'
            fileDir = os.path.join(outDir,f'State_{k}',fips)
            print(fileDir)
            print(countyBuildings.columns)
            if os.path.exists(fileDir) == False:
                os.mkdir(fileDir)
            countyBuildings.to_file(os.path.join(outDir,f'State_{k}',fips,layerName+'.gpkg'), driver='GPKG')
            print(os.path.join(outDir,f'State_{k}',fips,layerName+'.gpkg'))
        print(time.time()-start)