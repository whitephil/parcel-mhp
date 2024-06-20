import os
from glob import glob

data_dir = r'/scratch/alpine/phwh9568/parcelAtlas2021/ParcelAtlas2021'

zipList = glob(os.path.join(data_dir,'*.ZIP'))

print(zipList)

for zip in zipList:
    os.rename(zip, zip.replace('ZIP','zip'))