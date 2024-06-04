from zipfile import ZipFile
import os
import sys

fips = sys.argv[1].strip()
parcel_dir = r'/scratch/alpine/phwh9568/parcelAtlas2023/ParcelAtlas2023'
scratch = r'/scratch/alpine/phwh9568'

z = os.path.join(parcel_dir,fips+'.zip')

with ZipFile(z,'r') as zipped:
    stateDir = os.path.join(scratch,'State_' + z.split('/')[-1][0:2])
    countyDir = z.split("/")[-1].split('.')[0]
    zipped.extractall(os.path.join(stateDir,countyDir))
