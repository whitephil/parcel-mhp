import sys
import os
import parcelPipe

fips = sys.argv[1].strip()

data_dir = r'/scratch/alpine/phwh9568'

mhp_path = os.path.join(data_dir,'HIFLD_mhps.gpkg')

parcelPipe.mhp_splitter(fips,mhp_path)
