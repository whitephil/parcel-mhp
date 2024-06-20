import os
from zipfile import ZipFile
from glob import glob
import pandas as pd


def inventory(row, dataDir, fipsList):
    if f'{row.FIPS}.ZIP' in fipsList:
        try:
            ZipFile(os.path.join(dataDir,f'{row.FIPS}.ZIP'))
        except Exception as e:
            if str(e) == 'File is not a zip file':
                return False
        return True
    else:
        return False
    
def shpChk(row, ext, dataDir):
    if row.Parcels_Present == True:
        nameList = ZipFile(os.path.join(dataDir,f'{row.FIPS}.ZIP')).namelist()
        if f'{row.FIPS}/parcels.{ext}' in [n.lower() for n in nameList]:
            return True
        else:
            print(os.path.join(dataDir,f'{row.FIPS}.zip',f'parcels.{ext} is missing'))
            return False
    else:
        return 'NA'

dataDir = r'D:\ParcelAtlas'
outPut = 'inventory2021.csv'
countyFips = pd.read_csv(os.path.join(dataDir,'US_county_fips.csv'),encoding='utf-8', dtype={'FIPS':str})
dirList = glob(os.path.join(dataDir+'\*.ZIP'))
fipsList = [f.split("\\")[-1] for f in dirList]
print(fipsList)

exts = ['shp','dbf','shx','prj']

countyFips['Parcels_Present'] = countyFips.apply(lambda row: inventory(row, dataDir, fipsList), axis=1)

for ext in exts:
    countyFips[f'{ext}_Present'] = countyFips.apply(lambda row: shpChk(row, ext, dataDir), axis=1)

countyFips.to_csv(os.path.join(dataDir,outPut))


'''
zipped = ZipFile(os.path.join(dataDir,'01001.zip'))


nameList = zipped.namelist()

print(nameList)

if 'parcels.shp' in [n.lower() for n in nameList]:
'''