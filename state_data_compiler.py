import os
import geopandas as gpd
import pandas as pd
import pyogrio
import csv
import sys

data_dir = r'/scratch/alpine/phwh9568/'
sfips = sys.argv[1].strip()
state_dir = os.path.join(data_dir,f'State_{sfips}')
inventoryPath = os.path.join(data_dir,'inventory_All_Years.csv')

inventory = pd.read_csv(inventoryPath,dtype={'FIPS':str})
inventory = inventory.loc[inventory['Parcels_Present']==True]
stateInventory = inventory.loc[inventory['FIPS'].str.startswith(sfips)]
cfipsList = stateInventory['FIPS'].to_list()

outDir = os.path.join(state_dir,'state_outputs')
if os.path.exists(outDir)==False:
    os.mkdir(outDir)

versions = {'COSTAR':{'_parcels',
                    '_parc_blk_union2000',
                    '_parc_blk_union2010'}, 
            'HIFLD':{'_parcels',
                    '_parc_blk_union2000',
                    '_parc_blk_union2010'}}
years = ['2000','2010']

 
for version in versions.keys():    
    for year in years:
        yr = year[-2:]
        outDF = pd.DataFrame()      
        for fips in cfipsList:
            countyPath = os.path.join(state_dir,fips)               
            dtype={f'STATEFP{yr}':str,f'COUNTYFP{yr}':str,f'TRACTCE{yr}':str,f'BLOCKCE{yr}':str,f'GEOID{yr}':str,f'MTFCC{yr}':str,f'UACE{yr}':str,f'GEOID{yr}':str, 'MH_COUNTY_FIPS':str}
            if version == 'COSTAR':
                dtype.update({'MH_APN':str, 'APN':str, 'APN2':str})            
            filePath = os.path.join(countyPath,f'{version}_union_csv{year}.csv')            
            if os.path.exists(filePath):
                unionDF = pd.read_csv(filePath,dtype=dtype)
                outDF = pd.concat([outDF,unionDF])
        outDF.drop(outDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
        outDF.to_csv(os.path.join(outDir, f'{sfips}_{version}_{year}_final.csv'))

exceptionsFinalDF = pd.DataFrame()
for fips in cfipsList:
    countyEx = pd.read_csv(os.path.join(countyPath,'exceptions.csv'), dtype={'COUNTY_FIPS':str})
    exceptionsFinalDF = pd.concat([exceptionsFinalDF,countyEx])
exceptionsFinalDF.to_csv(os.path.join(outDir, f'{sfips}_exceptions.csv'))

# combine outputs into final state geodataframe    
for key, values in versions.items():
    for v in values:
        outDF = pd.DataFrame()
        for fips in cfipsList:
            gpkgPath = os.path.join(state_dir,fips,fips+'.gpkg')
            print(gpkgPath)
            if os.path.exists(gpkgPath):
                layers = pyogrio.list_layers(gpkgPath)
                print(layers)
                if f'{key}{v}' in layers:
                    print('dict:', f'{key}{v}')
                    outDF = pd.concat([outDF,gpd.read_file(os.path.join(gpkgPath),layer=f'{key}{v}')])
        #gdf = gpd.GeoDataFrame(outDF,geometry='geometry')
        outDF.to_file(os.path.join(outDir,f'{sfips}_Final.gpkg'),layer=f'{key}{v}')