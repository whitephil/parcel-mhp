import geopandas as gpd
import os
import csv
import sys
from zipfile import ZipFile
from glob import glob
import shutil
import warnings

gpd.options.io_engine = "pyogrio"
warnings.filterwarnings('ignore')

def areaComp(zipSHP,county):
    parcels = gpd.read_file(zipSHP)
    parcels.to_crs(crs='ESRI:102003', inplace=True)
    parcels.drop_duplicates(subset=['geometry'], inplace=True)
    county_area = county.iloc[0]['geometry'].area
    parcels_area = parcels['geometry'].area.sum()

    return (parcels_area/county_area)*100

def parcelAreaCheck(data_dir,year,fips,counties):
    zipPath = os.path.join(data_dir,fr'parcelAtlas{year}/ParcelAtlas{year}/{fips}.zip')
    county = counties.loc[counties['GEOID10']==fips].copy()
    print(zipPath)
    if os.path.exists(zipPath):
        nameList = ZipFile(zipPath).namelist()
        if f'{fips}/parcels.shp' in nameList:
            zipSHP = zipPath+f'!{fips}/parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        elif f'{fips}/Parcels.shp' in nameList:
            zipSHP = zipPath+f'!{fips}/Parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        elif f'parcels.shp' in nameList:
            zipSHP = zipPath+f'!parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        elif f'Parcels.shp' in nameList:
            zipSHP = zipPath+f'!Parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        else:
            areaDiff = False
            zipSHP = False
    else:
        areaDiff = False
        zipSHP = False
    return zipSHP, areaDiff

fips = sys.argv[1].strip()
#data_dir = r'/scratch/alpine/phwh9568'
data_dir = r'C:\Users\phwh9568\Data\ParcelAtlas'
countiesPath = os.path.join(data_dir,'tl_2010_us_county10.gpkg')

if fips[0:2] == '02':
    crs = 'EPSG:6393'
elif fips[0:2] == '15':
    crs = 'ESRI:102007'
else:
    crs = 'ESRI:102003'

counties = gpd.read_file(countiesPath, dtype={'STATEFP':str, 'COUNTYFP':str, 'GEOID10':str})
counties.to_crs(crs=crs, inplace=True)

years = ['2021','2022','2023']
areaDict = {}
for year in years:
    path, diff = parcelAreaCheck(data_dir,year,fips,counties)
    print(diff)
    if diff != False:
        areaDict[path] = diff
    else:
        continue

print(areaDict)

#parcelYear = min(areaDict,key=areaDict.get)

parcelPath = max(areaDict, key=areaDict.get)
diff = areaDict[parcelPath]
z, shpPath = parcelPath.split('!')
print('shpPath:', shpPath)

#parcel_dir = os.path.join(data_dir,f'parcelAtlas{parcelYear}/ParcelAtlas{parcelYear}')

#z = os.path.join(parcel_dir,fips+'.zip')
parcelYear = z.split('/')[-2][-4:]
print(parcelYear)
with ZipFile(z,'r') as zipped:
    stateDir = os.path.join(data_dir,'State_' + fips[0:2])
    countyDir = os.path.join(stateDir,fips)
    zipped.extractall(countyDir)
    if fips in shpPath:
        shps = glob(os.path.join(countyDir,fips,'*arcel*'))
        print(shps)
        for shp in shps:
            name = (shp.split('\\')[-1].lower())
            os.rename(shp, os.path.join(countyDir,name))
        shutil.rmtree(os.path.join(countyDir,fips))
    shps = glob(os.path.join(countyDir,'*arcel*'))
    for shp in shps:
        name = (shp.split('\\'))[-1].lower()
        os.rename(shp,os.path.join(countyDir,name))
            

    with open(os.path.join(stateDir,fips,'parcelYear.csv'),'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['COUNTY_FIPS','YEAR','PERC'])
        writer.writerow([fips,parcelYear,diff])