from glob import glob
from zipfile import ZipFile
import os

parcel_dir = r'/scratch/alpine/phwh9568/ParcelAtlas2023'

zips = glob(parcel_dir+'/*.zip')

for z in zips:
    with ZipFile(z,'r') as zipped:
        stateDir = 'State_' + z.split('/')[-1][0:2]
        countyDir = z.split("/")[-1].split('.')[0]
        zipped.extractall(os.path.join(stateDir,countyDir))
