import os
import geopandas as gpd
import pandas as pd
import csv
import numpy as np
from scipy import stats
import warnings

warnings.filterwarnings('ignore')

exceptPath = r'C:\Users\phwh9568\Data\ParcelAtlas\exceptions.csv'
outDir = r'C:\Users\phwh9568\Data\ParcelAtlas'

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


def sumWithin(parcels,buildings):
    #bespoke, only works with this data... but could be modified to be more flexible
    # parcels should be parcels, buildings shoudl be buildings
    parcels['ID'] = parcels.index
    cols = parcels.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    parcels = parcels[cols]
    buildings['areas'] = buildings['geometry'].area
    #print(buildings.columns)
    dfsjoin = gpd.sjoin(parcels,buildings,predicate='contains')
    #dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FINDEX':len,'areas':'mean'}).reset_index()
    dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FID':len,'areas':'mean'}).reset_index()
    dfpolynew = parcels.merge(dfpivot, how='left', on='ID')
    if 'FINDEX' in dfpolynew:
        dfpolynew.rename({'APN_x':'APN', 'FINDEX': 'Sum_Within', 'areas':'mnBlgArea'}, axis='columns',inplace=True)
        dfpolynew['Sum_Within'].fillna(0, inplace=True)
        dfpolynew['mnBlgArea'].fillna(0, inplace=True)
        dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)
    else:
        dfpolynew.rename({'APN_x':'APN'}, axis='columns',inplace=True)
        dfpolynew['Sum_Within'] = 0
        dfpolynew['mnBlgArea'] = 0
        dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)                
    return dfpolynew


# modified mhomes_prepper function to work with original MHP data
def mhomes_prepper(mhomesPath):
    mhomes = gpd.read_file(mhomesPath)
    mhomes.sindex
    columns = ['MHPID', 'NAME','ADDRESS', 'CITY', 'STATE', 'ZIP', 'STATUS', 'COUNTYFIPS', 'UNITS', 'SIZE', 'LATITUDE', 'LONGITUDE','geometry']
    renames = ['MHPID', 'MH_NAME','MH_ADDRESS', 'MH_CITY', 'MH_STATE', 'MH_ZIP', 'MH_STATUS','MH_COUNTY_FIPS', 'MH_UNITS', 'MH_SIZE', 'MH_LATITUDE', 'MH_LONGITUDE']
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
    concatted.drop(['tempID','IDcheck'], axis=1, inplace=True)
    return concatted



def parcelMHPJoin(pFilePath):    
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_COSTAR_mhps.gpkg')
    if os.path.exists(mhpPath):
        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
        parcel.to_crs(crs='ESRI:102003', inplace=True)
        #print(os.path.join(pFilePath,fips+'_Buildings.gpkg'))
        #buildings = gpd.read_file(os.path.join(pFilePath,fips+'_Buildings.gpkg'),layer=fips+'_Buildings')
        buildings = gpd.read_file(os.path.join(pFilePath,fips+'_buildings.shp'))
        buildings.to_crs(crs='ESRI:102003', inplace=True)
        mobileHomes = gpd.read_file(mhpPath, layer='COSTAR_mhps')
        mobileHomes.to_crs(crs='ESRI:102003', inplace=True)
        parcel = parcelPreSelect(parcel,buildings)   
        phomes = nearSelect(parcel,mobileHomes)
        if len(phomes) > 0:
            phomes.to_file(os.path.join(pFilePath,pFilePath.split('\\')[-1]+'.gpkg'),driver='GPKG', layer='MH_parcels')
    else:
        with open(exceptPath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([pFilePath.split('\\')[-1],'NO DIR'])


def union_intersect(pFilePath):
    fips = pFilePath.split('\\')[-1]
    if os.path.exists(os.path.join(pFilePath,fips+'.gpkg')) == True:
        phomes = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='MH_parcels')         
        blocks = gpd.read_file(os.path.join(pFilePath,fips+'_blocks.gpkg'), layer=fips+'_blocks')
        blocks.to_crs(crs='ESRI:102003', inplace=True)
        blocks['blockArea_m'] = blocks['geometry'].area
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
    if os.path.exists(os.path.join(pFilePath,'MHP_'+ pFilePath.split('\\')[-1] +'_COSTAR.csv')):
        mhp = pd.read_csv(os.path.join(pFilePath,'MHP_'+ pFilePath.split('\\')[-1] +'_COSTAR.csv'), dtype={'MH_COUNTY_FIPS':str, 'MH_parcel_num':str})
        if os.path.exists(os.path.join(pFilePath,'union_csv.csv')) == True:
            union = pd.read_csv(os.path.join(pFilePath,'union_csv.csv'), dtype={'GEOID10':str,'STATEFP10':str, 'COUNTYFP10':str, 'TRACTCE10':str,'BLOCKCE10':str, 'MH_parcel_num':str})
            mhp_union_merge = mhp.merge(union, on='MHPID', how = 'outer')
            #mhp_union_merge.drop(['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1, inplace=True)
            mhp_union_merge.drop(mhp_union_merge.filter(regex='_y$').columns, axis=1, inplace=True)
            renames_x = mhp_union_merge.filter(regex='_x$').columns
            renames = [x.split('_x')[0] for x in renames_x]
            renames = dict(zip(renames_x,renames))
            mhp_union_merge.rename(renames, axis='columns',inplace=True)
            mhp_union_merge.drop(mhp_union_merge.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp_union_merge.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_COSTAR_final.csv'))
        else:
            mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp.to_csv(os.path.join(pFilePath, 'MHP_'+ pFilePath.split('\\')[-1] +'_COSTAR_final.csv'))
