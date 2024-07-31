import os
import pandas as pd
import pyogrio
import csv

data_dir = r'/scratch/alpine/phwh9568/'
inventoryPath = os.path.join(data_dir,'inventory_All_Years.csv')

inventory = pd.read_csv(inventoryPath, dtype={'FIPS':str})
inventory = inventory.loc[inventory['Parcels_Present']==True]
inventory['STATEFP'] = inventory['FIPS'].str[0:2]
stateFips = inventory['STATEFP'].unique()

with open(os.path.join(data_dir,'data_inventory.csv'),'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['SFIPS','state','FIPS','parcels','blocks10','blocks00','buildings', 'COSTAR', 'HIFLD', 'RESULTS_GPKG', 'COSTAR_parcels', 'COSTAR_Un_lyr_00', 'COSTAR_Un_lyr_10', 'HIFLD_parcels', 'HIFLD_Un_lyr_00','HIFLD_Un_lyr_00', 'COSTAR_un_csv_00', 'COSTAR_un_csv_10', 'HIFLD_un_csv_00', 'HIFLD_un_csv_10'])
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

            if os.path.exists(os.path.join(state_dir,fips,f'{fips}_COSTAR_mhps.gpkg')):
                costar = True
            else:
                costar = False          

            if os.path.exists(os.path.join(state_dir,fips,f'{fips}_HIFLD_mhps.gpkg')):
                hifld = True
            else:
                hifld = False

            if os.path.exists(os.path.join(state_dir,fips,f'COSTAR_union_csv2000.csv')):
                costarUnion2000 = True
            else:
                costarUnion2000 = False  

            if os.path.exists(os.path.join(state_dir,fips,f'COSTAR_union_csv2010.csv')):
                costarUnion2010 = True
            else:
                costarUnion2010 = False  

            if os.path.exists(os.path.join(state_dir,fips,f'HIFLD_union_csv2000.csv')):
                hifldUnion2000 = True
            else:
                hifldUnion2000 = False              
            
            if os.path.exists(os.path.join(state_dir,fips,f'HIFLD_union_csv2010.csv')):
                hifldUnion2010 = True
            else:
                hifldUnion2010 = False  

            
            if os.path.exists(os.path.join(state_dir,fips,f'{fips}.gpkg')):
                resultsGPKG = True
                layers = pyogrio.list_layers(os.path.join(state_dir,fips,f'{fips}.gpkg'))
                if f'COSTAR_parc_blk_union2000' in layers:
                    costar00 = True
                else:
                    costar00 = False
                if f'COSTAR_parc_blk_union2010' in layers:
                    costar10 = True
                else:
                    costar10 = False
                if f'HIFLD_parc_blk_union2000' in layers:
                    hifld00 = True
                else:
                    hifld00 = False
                if f'HIFLD_parc_blk_union2010' in layers:
                    hifld10 = True
                else:
                    hifld10 = False
                if f'COSTAR_parcels' in layers:
                    costarParcels = True
                else:
                    costarParcels = False
                if f'HIFLD_parcels' in layers:
                    hifldParcels = True
                else:
                    hifldParcels = False
            else:
                resultsGPKG = False
                costar00 = False
                costar10 = False
                costarParcels = False
                hifld00 = False
                hifld10 = False
                hifldParcels = False

            writer.writerow([sfips,state,fips,parcels,blocks10,blocks00,buildings,costar,hifld,resultsGPKG,costarParcels,costar00,costar10,hifldParcels,hifld00,hifld10,costarUnion2000, costarUnion2010, hifldUnion2000,hifldUnion2010])