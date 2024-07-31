import os
import geopandas as gpd
import pandas as pd
import pyogrio
import csv
import sys

data_dir = r'/scratch/alpine/phwh9568/'
stateFips = pd.read_csv(os.path.join(data_dir,'stateFips.csv'), dtype={'STATEFP':str})
stateFipsList = stateFips['STATEFP'].to_list()

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
        nationDF = pd.DataFrame()      
        for fips in stateFipsList:
            stateDir = os.path.join(data_dir,f'State_{fips}')
            dtype={f'STATEFP{yr}':str,f'COUNTYFP{yr}':str,f'TRACTCE{yr}':str,f'BLOCKCE{yr}':str,f'GEOID{yr}':str,f'MTFCC{yr}':str,f'UACE{yr}':str,f'GEOID{yr}':str, 'MH_COUNTY_FIPS':str}
            if version == 'COSTAR':
                dtype.update({'MH_APN':str, 'APN':str, 'APN2':str})            
            filePath = os.path.join(stateDir,'state_outputs',f'{fips}_{version}_{year}_final.csv')
            if os.path.exists(filePath):
                stateDF = pd.read_csv(filePath,dtype=dtype)
                nationDF = pd.concat([nationDF,stateDF])
        nationDF.drop(nationDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
        nationDF.to_csv(os.path.join(data_dir, f'National_{version}_{year}_final.csv'))

for key, values in versions.items():
    for v in values:
        outDF = pd.DataFrame()
        for fips in stateFipsList:
            if fips == '02' or fips == '15':
                continue
            gpkgPath = os.path.join(data_dir,f'State_{fips}','state_outputs',f'{fips}_Final.gpkg')
            print(gpkgPath)
            if os.path.exists(gpkgPath):
                layers = pyogrio.list_layers(gpkgPath)
                print(layers)
                if f'{key}{v}' in layers:
                    print('dict:', f'{key}{v}')
                    outDF = pd.concat([outDF,gpd.read_file(os.path.join(gpkgPath),layer=f'{key}{v}')])
        #gdf = gpd.GeoDataFrame(outDF,geometry='geometry')
        outDF.to_file(os.path.join(data_dir,f'National_Final.gpkg'),layer=f'{key}{v}')

