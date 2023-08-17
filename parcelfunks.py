import os
import geopandas as gpd
import pandas as pd
import csv

mhomesPath = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\08_MHPs.gpkg'
blocksPath = r'C:\Users\phwh9568\Data\Census2020\tl_2020_08_all\tl_2020_08_tabblock10.shp'
exceptPath = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\exceptions.csv'

with open(exceptPath, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['COUNTY_FIPS','PROBLEM'])

# NEED to iterate over fips independent of folder structure to account for missing counties
# need to keep track of missing parcel data DONE
# need to keep track of bad parcel data 
# need to calculate match rate
# need to modify to get individual counties DONE
def mhomes_prepper(mhomesPath):
    mhomes = gpd.read_file(mhomesPath, layer='08_MHPs')
    #print(mhomes.columns)
    mhomes.sindex
    columns = ['USER_MHPID', 'USER_NAME','USER_ADDRE', 'USER_CITY', 'USER_STATE', 'USER_ZIP', 'USER_STATU', 'USER_COU_1','USER_LATIT', 'USER_LONGI','X','Y','geometry']
    renames = ['MHPID', 'MH_NAME','MH_ADDRESS', 'MH_CITY', 'MH_STATE', 'MH_ZIP', 'MH_STATUS','MH_COUNTY_FIPS','MH_LATITUDE', 'MH_LONGITUDE','MH_Geocoded_X','MH_Geocoded_Y']
    drops = [c for c in mhomes.columns if c not in columns] 
    renames = dict(zip(columns,renames))
    #print(mhomes.columns)
    mhomes.drop(drops,axis=1, inplace=True)
    mhomes.rename(renames, axis='columns',inplace=True)
    #print(mhomes.columns)
    return mhomes

mobileHomes = mhomes_prepper(mhomesPath)

def parcelMHPJoin(pFilePath):    
    print(pFilePath)

    parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
    parcel.drop_duplicates(subset=['APN'], inplace=True)
    columns = ['APN', 'APN2', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    if parcel.crs != mobileHomes.crs:
        parcel.to_crs(mobileHomes.crs, inplace=True)
    phomes = gpd.sjoin(parcel,mobileHomes)
    phomes.drop('index_right', axis=1, inplace=True)
    if len(phomes) > 0:
        phomes.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'),driver='GPKG', layer='MH_parcels')
    else:
        with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([pFilePath.split('\\')[-1],'NO JOIN'])

def blocks_prepper(blocksPath):
    blocks = gpd.read_file(blocksPath)
    blocksAlbers = blocks.to_crs(crs='ESRI:102003')
    blocksAlbers['blockArea_m'] = blocksAlbers['geometry'].area
    return blocksAlbers

blocksAlbers = blocks_prepper(blocksPath)

def union_intersect(pFilePath):
    if os.path.exists(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg')) == True:
        phomes = gpd.read_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'), layer='MH_parcels') 
        phomesAlbers = phomes.to_crs(blocksAlbers.crs)
        union = blocksAlbers.overlay(phomesAlbers, how='intersection')
        union['unionArea_m'] = union['geometry'].area
        union['blockParcel_ratio'] = (union['unionArea_m']/union['blockArea_m']) *100
        if len(union) > 0:
            union.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'),driver='GPKG', layer='MH_parc_blk_union')
            union.to_csv(os.path.join(pFilePath,'union_csv.csv'))
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO UNION'])

# need to get this to create a clean output
def mhp_union_merge(pFilePath):
    if os.path.exists(os.path.join(pFilePath,'MHP_'+ pFilePath.split('\\')[-1] +'.csv')):
        mhp = pd.read_csv(os.path.join(pFilePath,'MHP_'+ pFilePath.split('\\')[-1] +'.csv'), dtype={'MH_COUNTY_FIPS':str, 'MHPID':str})
    if os.path.exists(os.path.join(pFilePath,'union_csv.csv')) == True:
        union = pd.read_csv(os.path.join(pFilePath,'union_csv.csv'), dtype={'GEOID10':str,'STATEFP10':str, 'COUNTYFP10':str, 'TRACTCE10':str,'BLOCKCE10':str, 'MHPID':str})
        mhp_union_merge = mhp.merge(union, on='MHPID', how = 'outer')
        #mhp_union_merge.drop(['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1, inplace=True)
        mhp_union_merge.drop(mhp_union_merge.filter(regex='_y$').columns, axis=1, inplace=True)
        renames_x = mhp_union_merge.filter(regex='_x$').columns
        renames = [x.split('_x')[0] for x in renames_x]
        renames = dict(zip(renames_x,renames))
        mhp_union_merge.rename(renames, axis='columns',inplace=True)
        mhp_union_merge.drop(mhp_union_merge.filter(regex='Unnamed*').columns,axis=1, inplace=True)
        mhp_union_merge.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_final.csv'))
    else:
        mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
        mhp.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_final.csv'))
