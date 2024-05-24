import os
import pandas as pd

data_dir = r'F:\ParcelAtlas2023'
inventory = pd.read_csv(os.path.join(data_dir,'inventory.csv'),dtype={'FIPS':str})

inventory = inventory.loc[inventory['Parcels_Present']==True]
inventory = inventory.loc[inventory['FIPS'].str.startswith('08')]

fips = inventory['FIPS'].to_list()

txt_path = os.path.join(data_dir, 'counties.txt')

with open(txt_path, 'w') as f:
    for fip in fips:
        f.write()
