import os
import pandas as pd
import pyogrio
import csv

data_dir = r'/scratch/alpine/phwh9568/'
inventoryPath = os.path.join(data_dir,'inventory.csv')

inventory = pd.read_csv(inventoryPath, dtype={'FIPS':str})
inventory = inventory.loc[inventory['Parcels_Present']==True]
inventory['STATEFP'] = inventory['FIPS'].str[0:2]
stateFips = inventory['STATEFP'].unique()

with open(os.path.join(data_dir,'data_inventory.csv'),'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['SFIPS','state','FIPS','parcels','blocks10','blocks00','buildings'])
    for sfips in stateFips:
        state_dir = os.path.join(data_dir,f'State_{sfips}')
        if os.path.exists(state_dir):
            state = True
        else:
            state = False
        cfips = inventory.loc[inventory['STATEFP']==sfips]
        cfips = cfips['FIPS'].to_list()
        
        for fips in cfips:
        
            if os.path.exists(os.path.join(state_dir,fips,'parcels.shp')):
                parcels = True
            else:
                parcels = False

            if os.path.exists(os.path.join(state_dir,fips,f'{fips}_blocks.gpkg')):
                blockLayers = pyogrio.list_layers(os.path.join(state_dir,fips,f'{fips}_blocks.gpkg'))
                if f'{fips}_blocks10' in blockLayers:
                    blocks10 = True
                else:
                    blocks10 = False
                if f'{fips}_blocks00' in blockLayers:
                    blocks00 = True
                else:
                    blocks00 = False
            else:
                blocks10 = False
                blocks00 = False
            
            if os.path.exists(os.path.join(state_dir,fips,f'{fips}_Buildings.gpkg')):
                buildings = True
            else:
                buildings = False

            writer.writerow([sfips,state,fips,parcels,blocks10,blocks00,buildings])