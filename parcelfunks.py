import os
import geopandas as gpd
import pandas as pd
import csv
import numpy as np
from scipy import stats

#temporarily silence runtimewarning
import warnings

def fxn():
    warnings.warn("deprecated", RuntimeWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()

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
    if geom.geom_type == 'Polygon':
        return sum([len(g.xy[0]) for g in geom.interiors]) if len(geom.interiors) > 0 else 0
    if geom.geom_type == 'MultiPolygon':
        multiGeoms = geom.geoms
        return sum([sum([len(g.xy[0]) for g in mg.interiors]) if len(mg.interiors) > 0 else 0 for mg in multiGeoms])

def exteriorLen(geom):
    '''
    sum of exterior points in a polygon
    '''
    if geom.geom_type == 'Polygon':
        return len(geom.exterior.xy[0])
    if geom.geom_type == 'MultiPolygon':
        return sum([len(g.exterior.xy[0]) for g in geom.geoms])

# not using delete
def geomLen(geom):
    '''
    sum of total exterior and interior points in a polygon
    '''
    exterior = len(geom.exterior.xy[0])
    interior = sum([len(g.xy[0]) for g in geom.interiors]) if len(geom.interiors) > 0 else 0
    return exterior+interior

def sumWithin(df1,df2):
    #bespoke, only works with this data... but could be modified to be more flexible
    # df1 should be parcels, df2 shoudl be buildings
    df1['ID'] = df1.index
    cols = df1.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    df1 = df1[cols]
    df2['areas'] = df2['geometry'].area
    dfsjoin = gpd.sjoin(df1,df2,predicate='contains')
    dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FID':len,'areas':'mean'}).reset_index()
    dfpolynew = df1.merge(dfpivot, how='left', on='ID')
    if 'FID' in dfpolynew:
        dfpolynew.rename({'APN_x':'APN', 'FID': 'Sum_Within', 'areas':'mnBlgArea'}, axis='columns',inplace=True)
        dfpolynew['Sum_Within'].fillna(0, inplace=True)
        dfpolynew['mnBlgArea'].fillna(0, inplace=True)
        dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)
    else:
        dfpolynew.rename({'APN_x':'APN'}, axis='columns',inplace=True)
        dfpolynew['Sum_Within'] = 0
        dfpolynew['mnBlgArea'] = 0
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

def parcelPreSelect(parcel,buildings):
    parcel.drop_duplicates(subset=['geometry'], inplace=True)
    parcel = sumWithin(parcel,buildings)
    parcel['geometry'] = parcel['geometry'].simplify(1)
    parcel['intLen'] = parcel.apply(lambda row: interiorLen(row.geometry), axis=1)
    parcel['intZscore'] = np.abs(stats.zscore(parcel['intLen']))
    parcel.drop(parcel[parcel.intLen >= 20].index, inplace=True) #dropping outlier inner geometries
    parcel.reset_index(inplace=True)
    parcel['extLen1'] = parcel.apply(lambda row: exteriorLen(row.geometry), axis=1)
    parcel['extZscore1'] = np.abs(stats.zscore(parcel['extLen1']))
    parcel.drop(parcel[(parcel['extZscore1'] > 3) & (parcel['Sum_Within'] < 10)].index, inplace=True) #dropping outlier geometries
    parcel.drop(parcel[parcel['Sum_Within'] < 5].index, inplace=True) #change to 20 or 30 later
    parcel.drop(parcel[parcel['mnBlgArea'] > 175].index, inplace=True)
    parcel.reset_index(inplace=True)
    columns = ['APN', 'APN2', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    return parcel


def nearSelect(parcel, mobileHomes):
    cols = parcel.columns
    parcel['tempID'] = parcel.index
    mhps_parcels_near = gpd.sjoin_nearest(mobileHomes,parcel,max_distance=100, distance_col='distances')
    mhps_parcels_near.drop(cols, axis=1, inplace=True)
    merged = parcel.merge(mhps_parcels_near, on='tempID')
    merged.drop(['index_right'], axis=1, inplace=True)
    merged = merged.sort_values(['tempID','distances']).drop_duplicates(subset=['tempID'], keep='first')
    parcel['IDcheck'] = parcel['tempID'].isin(merged['tempID'].to_list())
    unmatched = parcel.loc[parcel['IDcheck']==False]
    secondJoin = gpd.sjoin_nearest(unmatched, mobileHomes, max_distance=150.0, distance_col='distances')
    secondJoin.drop('index_right',axis=1,inplace=True)
    concatted = pd.concat([merged,secondJoin])
    concatted.drop(['tempID'], axis=1, inplace=True)
    return concatted



def parcelMHPJoin(pFilePath):    
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_mhps_OG.gpkg')
    if os.path.exists(mhpPath):
        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
        parcel.to_crs(crs='ESRI:102003', inplace=True)
        buildings = gpd.read_file(os.path.join(pFilePath,fips+'_buildings.shp'))
        buildings.to_crs(crs='ESRI:102003', inplace=True)
        mobileHomes = gpd.read_file(mhpPath)
        mobileHomes.to_crs(crs='ESRI:102003', inplace=True)
        parcel = parcelPreSelect(parcel,buildings)   
        phomes = nearSelect(parcel,mobileHomes)
        if len(phomes) > 0:
            phomes.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'double.gpkg'),driver='GPKG', layer='MH_parcels')
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO JOIN'])



def parcelMHPJoin2(pFilePath):
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_mhps_OG.gpkg')
    if os.path.exists(mhpPath):
        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
        parcel.to_crs(crs='ESRI:102003', inplace=True)
        parcel.drop_duplicates(subset=['geometry'], inplace=True)
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
        parcel.drop(parcel[(parcel['extZscore1'] > 3) & (parcel['Sum_Within'] < 10)].index, inplace=True) #dropping outlier geometries
        parcel.drop(parcel[parcel['Sum_Within'] < 1].index, inplace=True)
        parcel.reset_index(inplace=True)
        columns = ['APN', 'APN2', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
        drops = [c for c in parcel.columns if c not in columns]
        parcel.drop(drops, axis=1, inplace=True)        
        mobileHomes = gpd.read_file(mhpPath)
        mobileHomes.to_crs(crs='ESRI:102003', inplace=True) 
        if parcel.crs != mobileHomes.crs:
            parcel.to_crs(mobileHomes.crs, inplace=True)
        phomes = gpd.sjoin_nearest(parcel, mobileHomes, max_distance=100.0, distance_col='distances')
        phomes.drop('index_right', axis=1, inplace=True)
        #phomes = phomes.sort_values(['MHPID','Sum_Within'], ascending=False).drop_duplicates(subset=['MHPID'], keep='first')
        phomes.drop(phomes[(phomes['Sum_Within'] < 10) & (phomes['mnBlgArea'] > 175)].index, inplace=True)
        phomes.drop(phomes[phomes['mnBlgArea'] > 200].index, inplace=True)
        phomes.drop(phomes[phomes['Sum_Within'] < 5].index, inplace=True) #drop matched parcels with less than 5 buildings
        #phomes.drop_duplicates(subset=['geometry'], inplace=True)
        if len(phomes) > 0:
            phomes.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'multi100_less5.gpkg'),driver='GPKG', layer='MH_parcels')
        else:
            with open(exceptPath, 'a', newline='', encoding='utf-8') as f: #need to figure out a better way. Is it overwriting?
                writer = csv.writer(f)
                writer.writerow([pFilePath.split('\\')[-1],'NO JOIN'])
       

def union_intersect(pFilePath):
    fips = pFilePath.split('\\')[-1]
    if os.path.exists(os.path.join(pFilePath,fips+'double.gpkg')) == True:
        phomes = gpd.read_file(os.path.join(pFilePath,fips+'double.gpkg'), layer='MH_parcels')         
        blocks = gpd.read_file(os.path.join(pFilePath,fips+'_blocks.gpkg'), layer=fips+'_blocks')
        #phomesAlbers = phomes.to_crs(blocksAlbers.crs)
        union = blocks.overlay(phomes, how='intersection')
        union['unionArea_m'] = union['geometry'].area
        union['blockParcel_ratio'] = (union['unionArea_m']/union['blockArea_m']) *100
        if len(union) > 0:
            union.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'double.gpkg'),driver='GPKG', layer='MH_parc_blk_union')
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
            mhp_union_merge.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'double.csv'))
        else:
            mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'double.csv'))

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