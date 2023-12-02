import os
import geopandas as gpd
import pandas as pd
import csv
import numpy as np
from scipy import stats

mhomesPath = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\08_MHPs.gpkg'
mhomesPath2 = r'C:\Users\phwh9568\Data\ParcelAtlas\Mobile_Home_Parks\MobileHomeParks.shp'
blocksPath = r'C:\Users\phwh9568\Data\Census2020\tl_2020_08_all\tl_2020_08_tabblock10.shp'
exceptPath = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\exceptions.csv'

with open(exceptPath, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['COUNTY_FIPS','PROBLEM'])

def interiorLen(geom):
    '''
    sum of interior points in a polygon
    '''
    return sum([len(g.xy[0]) for g in geom.interiors]) if len(geom.interiors) > 0 else 0

def exteriorLen(geom):
    '''
    sum of exterior points in a polygon
    '''
    return len(geom.exterior.xy[0])

def geomLen(geom):
    '''
    sum of total exterior and interior points in a polygon
    '''
    exterior = len(geom.exterior.xy[0])
    interior = sum([len(g.xy[0]) for g in geom.interiors]) if len(geom.interiors) > 0 else 0
    return exterior+interior

def sumWithin(df1,df2):
    #bespoke, only works with this data... but could be modified to be more flexible
    df1['ID'] = df1.index
    cols = df1.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    df1 = df1[cols]
    dfsjoin = gpd.sjoin(df1,df2,predicate='contains')
    dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FID':len}).reset_index()
    dfpolynew = df1.merge(dfpivot, how='left', on='ID')
    dfpolynew.rename({'APN_x':'APN', 'FID': 'Sum_Within'}, axis='columns',inplace=True)
    dfpolynew['Sum_Within'].fillna(0, inplace=True)
    dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)
    return dfpolynew

# NOT BEING USED DELETE THIS
def geomZscore(geom):
    return stats.zscore(len(geom.exterior.xy[0]))



# modified functions for original mobile home park data:

# modified mhomes_prepper function to work with original MHP data
def mhomes_prepper(mhomesPath):
    mhomes = gpd.read_file(mhomesPath, layer='08_MHPs_OG')
    mhomes.sindex
    columns = ['MHPID', 'NAME','ADDRESS', 'CITY', 'STATE', 'ZIP', 'STATUS', 'COUNTYFIPS','LATITUDE', 'LONGITUDE','geometry']
    renames = ['MHPID', 'MH_NAME','MH_ADDRESS', 'MH_CITY', 'MH_STATE', 'MH_ZIP', 'MH_STATUS','MH_COUNTY_FIPS','MH_LATITUDE', 'MH_LONGITUDE']
    drops = [c for c in mhomes.columns if c not in columns] 
    renames = dict(zip(columns,renames))
    mhomes.drop(drops,axis=1, inplace=True)
    mhomes.rename(renames, axis='columns',inplace=True)
    return mhomes

#mobileHomes = mhomes_prepper2(mhomesPath2)

def parcelMHPJoin(pFilePath):    
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_mhps_OG.gpkg')
    if os.path.exists(mhpPath):
        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
        parcel.to_crs(crs='ESRI:102003', inplace=True)
        parcel.drop_duplicates(subset=['geometry'], inplace=True)
        parcel = parcel.explode(index_parts=False)
        buildings = gpd.read_file(os.path.join(pFilePath,fips+'_buildings.shp'))
        buildings.to_crs(crs='ESRI:102003', inplace=True)
        parcel = sumWithin(parcel,buildings)
        parcel['geometry'] = parcel['geometry'].simplify(1)
        parcel['intLen'] = parcel.apply(lambda row: interiorLen(row.geometry), axis=1)
        parcel['intZscore'] = np.abs(stats.zscore(parcel['intLen']))
        parcel.drop(parcel[parcel.intLen >= 20].index, inplace=True) #dropping outlier inner geometries
        parcel.reset_index(inplace=True)
        parcel['extLen1'] = parcel.apply(lambda row: exteriorLen(row.geometry), axis=1)
        parcel['extZscore1'] = np.abs(stats.zscore(parcel['extLen1']))
        #parcel.drop(parcel[parcel.geomZscore1 > 10].index, inplace=True) #dropping outlier geometries
        #parcel.reset_index(inplace=True)
        #parcel['geometry'] = parcel['geometry'].simplify(3)
        #parcel['polyLen2'] = parcel.apply(lambda row: geomLen(row.geometry), axis=1)
        #parcel['geomZscore2'] = np.abs(stats.zscore(parcel['polyLen2']))
        #parcel.drop(parcel[parcel.geomZscore > 3].index, inplace=True) #dropping outlier geometries
        #parcel.reset_index(inplace=True)
        columns = ['APN', 'APN2', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within','geometry']
        drops = [c for c in parcel.columns if c not in columns]
        parcel.drop(drops, axis=1, inplace=True)        
        mobileHomes = gpd.read_file(mhpPath)
        mobileHomes.to_crs(crs='ESRI:102003', inplace=True)
        if parcel.crs != mobileHomes.crs:
            parcel.to_crs(mobileHomes.crs, inplace=True)
        phomes = gpd.sjoin_nearest(parcel, mobileHomes, max_distance=5.0, distance_col='distances')
        phomes.drop('index_right', axis=1, inplace=True)
        #phomes = phomes.sort_values(['MHPID','distances']).drop_duplicates(subset=['MHPID'], keep='first')
        #phomes['polyLen2'] = phomes.apply(lambda row: geomLen(row.geometry), axis=1)
        #phomes['geomZscore3'] = np.abs(stats.zscore(phomes['polyLen2']))
        #phomesAlbers = phomes.to_crs(crs='ESRI:102003')
        if len(phomes) > 0:
            phomes.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'),driver='GPKG', layer='MH_parcels')
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO JOIN'])

def union_intersect(pFilePath):
    fips = pFilePath.split('\\')[-1]
    if os.path.exists(os.path.join(pFilePath,fips+'.gpkg')) == True:
        phomes = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='MH_parcels')         
        blocks = gpd.read_file(os.path.join(pFilePath,fips+'_blocks.gpkg'), layer=fips+'_blocks')
        #phomesAlbers = phomes.to_crs(blocksAlbers.crs)
        union = blocks.overlay(phomes, how='intersection')
        union['unionArea_m'] = union['geometry'].area
        union['blockParcel_ratio'] = (union['unionArea_m']/union['blockArea_m']) *100
        if len(union) > 0:
            union.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'),driver='GPKG', layer='MH_parc_blk_union')
            union.to_csv(os.path.join(pFilePath,'union_csv.csv'))
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO UNION'])


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
            mhp_union_merge.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_final_near.csv'))
        else:
            mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_final_near.csv'))

'''
# NEED to iterate over fips independent of folder structure to account for missing counties
# need to keep track of missing parcel data DONE
# need to keep track of bad parcel data 
# need to calculate match rate
# need to modify to get individual counties DONE
def mhomes_prepper(mhomesPath):
    mhomes = gpd.read_file(mhomesPath, layer='08_MHPs')
    mhomes.sindex
    columns = ['USER_MHPID', 'USER_NAME','USER_ADDRE', 'USER_CITY', 'USER_STATE', 'USER_ZIP', 'USER_STATU', 'USER_COU_1','USER_LATIT', 'USER_LONGI','X','Y','geometry']
    renames = ['MHPID', 'MH_NAME','MH_ADDRESS', 'MH_CITY', 'MH_STATE', 'MH_ZIP', 'MH_STATUS','MH_COUNTY_FIPS','MH_LATITUDE', 'MH_LONGITUDE','MH_Geocoded_X','MH_Geocoded_Y']
    drops = [c for c in mhomes.columns if c not in columns] 
    renames = dict(zip(columns,renames))
    mhomes.drop(drops,axis=1, inplace=True)
    mhomes.rename(renames, axis='columns',inplace=True)
    return mhomes

#mobileHomes = mhomes_prepper(mhomesPath)



#mobileHomes = gpd.read_file(mhomesPath, layer='08_MHPs_Prepped')

def parcelMHPJoin(pFilePath):    
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_mhps.gpkg')
    if os.path.exists(mhpPath):
        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
        parcel.drop_duplicates(subset=['APN'], inplace=True)
        parcel = parcel.explode(index_parts=False)
        parcel['polyLen'] = parcel.apply(lambda row: geomLen(row.geometry), axis=1)
        parcel['geomZscore'] = np.abs(stats.zscore(parcel['polyLen']))
        parcel.drop(parcel[parcel.geomZscore > 3].index, inplace=True) #dropping outlier geometries
        parcel.reset_index(inplace=True)
        columns = ['APN', 'APN2', 'geometry']
        drops = [c for c in parcel.columns if c not in columns]
        parcel.drop(drops, axis=1, inplace=True)        
        mobileHomes = gpd.read_file(mhpPath)
        if parcel.crs != mobileHomes.crs:
            parcel.to_crs(mobileHomes.crs, inplace=True)
        phomes = gpd.sjoin(parcel,mobileHomes)
        phomes.drop('index_right', axis=1, inplace=True)
        phomesAlbers = phomes.to_crs(crs='ESRI:102003')
        if len(phomesAlbers) > 0:
            phomesAlbers.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer='MH_parcels')
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO JOIN'])

def blocks_prepper(blocksPath):
    blocks = gpd.read_file(blocksPath)
    blocksAlbers = blocks.to_crs(crs='ESRI:102003')
    blocksAlbers['blockArea_m'] = blocksAlbers['geometry'].area
    return blocksAlbers
# CHANGE THIS TO READ IN INDIVIDUAL BLOCKS LAYERS FROM COUNTY DIRS
#blocksAlbers = blocks_prepper(blocksPath)

def union_intersect(pFilePath):
    fips = pFilePath.split('\\')[-1]
    if os.path.exists(os.path.join(pFilePath,fips+'.gpkg')) == True:
        phomesAlbers = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='MH_parcels')         
        blocksAlbers = gpd.read_file(os.path.join(pFilePath,fips+'_blocks.gpkg'), layer=fips+'_blocks')
        #phomesAlbers = phomes.to_crs(blocksAlbers.crs)
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
'''