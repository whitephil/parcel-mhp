import os
import pandas as pd
import sys

data_dir = r'/scratch/alpine/phwh9568/'
inventoryPath = os.path.join(data_dir,'inventory_All_Years.csv')
inventory = pd.read_csv(inventoryPath, dtype={'FIPS':str})
inventory = inventory.loc[inventory['Parcels_Present']==True]
inventory['STATEFP'] = inventory['FIPS'].str[0:2]
stateFips = inventory['STATEFP'].unique()

inventoryFinal = pd.DataFrame()
for sfips in stateFips:
    state_dir = os.path.join(data_dir,f'State_{sfips}')
    stateInv = pd.read_csv(os.path.join(state_dir,'state_inventory.csv'), dtype={'SFIPS':str, 'FIPS':str})
    inventoryFinal = pd.concat([inventoryFinal, stateInv])
inventoryFinal.to_csv(os.path.join(data_dir,'data_inventory.csv'))