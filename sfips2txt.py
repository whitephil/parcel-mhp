import os
import pandas as pd

data_dir = r'F:\ParcelAtlas2023'
state_path = 'stateFips.csv'

stateFips = pd.read_csv(os.path.join(data_dir,state_path),dtype={'STATEFP':str})

fips = stateFips['STATEFP'].to_list()

txt_path = os.path.join(data_dir,'State_Fips.txt')

with open(txt_path,'w') as f:
    for fip in fips:
        f.write(fip+'\n')