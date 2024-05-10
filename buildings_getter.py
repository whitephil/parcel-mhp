import geopandas as gpd
import pandas as pd
import requests
import os
import warnings
import sys


sfips = sys.argv[1]
warnings.filterwarnings("ignore")
gpd.options.io_engine = "pyogrio"

dataDir = r'C:\Users\phwh9568\Data\ParcelAtlas'
countiesPath = os.path.join(dataDir,'tl_2022_us_county.gpkg')
outDir = os.path.join(dataDir,'states',f'State_{sfips}')
if os.path.exists(outDir)==False:
    os.mkdir(outDir)

url = 'https://usbuildingdata.blob.core.windows.net/usbuildings-v2/'

counties = gpd.read_file(countiesPath, dtype={'STATEFP':str, 'COUNTYFP':str, 'GEOID':str})
stateFips = pd.read_csv(os.path.join(dataDir,'stateFips.csv'), dtype={'STATEFP':str, 'STATENS':str})
name = stateFips.loc[stateFips['STATEFP']==sfips]['STATE_NAME'].values[0]

stateUrl = url+name.replace(' ','')+'.geojson.zip'
stateBuildings = gpd.read_file(requests.get(stateUrl).content)
stateBuildings['FINDEX'] = stateBuildings.index
stateCounties = counties.loc[counties['STATEFP'] == sfips]
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
    countyBuildings.to_crs(crs='ESRI:102005')
    layerName = fips+'_Buildings'
    fileDir = os.path.join(outDir,fips)
    print(fileDir)
    print(countyBuildings.columns)
    if os.path.exists(fileDir) == False:
        os.mkdir(fileDir)
    countyBuildings.to_file(os.path.join(fileDir,layerName+'.gpkg'), driver='GPKG')
    print(os.path.join(outDir,fips,layerName+'.gpkg'))