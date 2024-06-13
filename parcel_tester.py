import geopandas as gpd
import os
import csv
import sys
from zipfile import ZipFile

def areaComp(zipSHP,county):
    parcels = gpd.read_file(zipSHP)
    parcels.to_crs(crs='ESRI:102003', inplace=True)
    county_area = county.iloc[0]['geometry'].area
    parcels_area = parcels['geometry'].area.sum()

    return abs(county_area-parcels_area)

def parcelAreaCheck(data_dir,year,fips,counties):
    zipPath = os.path.join(data_dir,fr'parcelAtlas{year}/ParcelAtlas{year}/{fips}.zip')
    county = counties.loc[counties['GEOID10']==fips].copy()
    print(len(county))
    if os.path.exists(zipPath):
        if f'{fips}/parcels.shp' in ZipFile(zipPath).namelist():
            zipSHP = zipPath+f'!{fips}/parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        elif f'{fips}/Parcels.shp' in ZipFile(zipPath).namelist():
            zipSHP = zipPath+f'!{fips}/Parcels.shp'
            areaDiff = areaComp(zipSHP,county)
        else:
            areaDiff = False
    else:
        areaDiff = False
    return areaDiff

fips = sys.argv[1].strip()
#data_dir = r'/scratch/alpine/phwh9568'
data_dir = r'C:\Users\phwh9568\Data\ParcelAtlas'
countiesPath = os.path.join(data_dir,'tl_2010_us_county10.gpkg')
crs = 'ESRI:102003'

counties = gpd.read_file(countiesPath, dtype={'STATEFP':str, 'COUNTYFP':str, 'GEOID10':str})
counties.to_crs(crs=crs, inplace=True)

years = ['2021','2022','2023']
areaDict = {}
for year in years:
    diff = parcelAreaCheck(data_dir,year,fips,counties)
    print(diff)
    if diff != False:
        areaDict[year] = diff
    else:
        continue

parcelYear = min(areaDict,key=areaDict.get)

parcel_dir = os.path.join(data_dir,f'parcelAtlas{parcelYear}/ParcelAtlas{parcelYear}')

z = os.path.join(parcel_dir,fips+'.zip')

with ZipFile(z,'r') as zipped:
    stateDir = os.path.join(data_dir,'State_' + z.split('/')[-1][0:2])
    countyDir = z.split("/")[-1].split('.')[0]
    zipped.extractall(os.path.join(stateDir,countyDir))
    
    with open(os.path.join(countyDir,'parcelYear.csv'),'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['COUNTY_FIPS','YEAR'])
        writer.writerow([fips,parcelYear])