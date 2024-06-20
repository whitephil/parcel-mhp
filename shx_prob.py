import geopandas as gpd
from osgeo import gdal
import os
import warnings
import sys

#gpd.options.io_engine = "pyogrio"
warnings.filterwarnings('ignore')
gdal.SetConfigOption('SHAPE_RESTORE_SHX', 'YES')

data_dir = r'F:\ParcelAtlas2023'

fips = sys.argv[1].strip()

gpd.read_file(os.path.join(data_dir,fips,fips,'parcels.shp'))
