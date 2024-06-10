import os
import geopandas as gpd
import parcelPipe

gpd.options.io_engine = 'pyogrio'

mhp_dir = r"C:\Users\phwh9568\Data\ParcelAtlas\Mobile_Home_Parks_6_24"

mhp_path = os.path.join(mhp_dir,'MobileHomeParks.shp')

mhps = parcelPipe.mhomes_prepper(mhp_path)

mhps.to_file(os.path.join(mhp_dir,'HIFLD_mhps.gpkg'),driver='GPKG')