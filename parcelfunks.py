import os
import fiona
import geopandas as gpd
import pandas as pd
import csv
import numpy as np
from scipy import stats
import warnings

warnings.filterwarnings('ignore')

exceptPath = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\exceptions.csv'
outDir = r'C:\Users\phwh9568\Data\ParcelAtlas'

with open(exceptPath, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['COUNTY_FIPS','PROBLEM'])


# Filtering out outlier parcels:
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

def unitCheck(phomes):
    #first remove any duplicate parcel/distances (likely zero--a 0 dist from apn join and wrong 0 dist from incorrect spatial join due to inaccurate geocode)
    #phomes = phomes.sort_values(by=['MH_prop_id','distances','APN_JOIN']).drop_duplicates(subset=['distances','MH_prop_id'], keep='last')
    #note that this drop based on duplicated 
    piv = pd.pivot_table(phomes,index=['MH_APN','MH_units'], aggfunc={'Sum_Within':'sum', 'MH_prop_id':len}).reset_index()
    if len(piv) > 0:    
        piv.rename({'Sum_Within':'Total_Blds', 'MH_prop_id':'MH_Parcel_Count'},axis=1, inplace=True)
        piv['BLD_UNIT_MARGIN'] = (100 - ((piv['MH_units']/piv['Total_Blds'])*100))
        piv['flag'] = np.where((piv['MH_Parcel_Count'] > 1) & (piv['BLD_UNIT_MARGIN'] > 15),1,0)
        phomes = pd.merge(phomes,piv[['MH_APN','MH_Parcel_Count','Total_Blds', 'BLD_UNIT_MARGIN', 'flag']],how='left', on='MH_APN')
    else:
        phomes = phomes.assign(MH_Parcel_Count =np.nan, Total_Blds=np.nan, BLD_UNIT_MARGIN=np.nan, flag=np.nan)
    return phomes

def rankNdrop(phomes):
    #now rank and remove furthest if building sum if building margin is high and positive
    phomes['rank'] = phomes.groupby('MH_APN')['distances'].rank(method='max')
    phomes.drop(phomes[(phomes['flag']==True) & (phomes['MH_Parcel_Count'] == phomes['rank'])].index, inplace=True)
    phomes.drop(columns={'MH_Parcel_Count', 'Total_Blds', 'BLD_UNIT_MARGIN', 'flag','rank'}, inplace=True)
    return(phomes)

def unitFilter(phomes):
    phomes = unitCheck(phomes)
    while 1 in phomes['flag'].values:
        phomes = rankNdrop(phomes)
        phomes = unitCheck(phomes)
    phomes.drop(columns={'flag'}, inplace=True)
    return(phomes)

# Buildings
def sumWithin(parcels,buildings):
    #bespoke, only works with this data... but could be modified to be more flexible
    # parcels should be parcels, buildings shoudl be buildings
    parcels['ID'] = parcels.index
    cols = parcels.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    parcels = parcels[cols]
    buildings['areas'] = buildings['geometry'].area
    #print(buildings.columns)
    dfsjoin = gpd.sjoin(parcels,buildings,predicate='intersects')
    #dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FINDEX':len,'areas':'mean'}).reset_index()
    dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FID':len,'areas':'mean'}).reset_index()
    dfpolynew = parcels.merge(dfpivot, how='left', on='ID')
    if 'FID' in dfpolynew: #remember to switch to 'FINDEX' when running in future
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


# UPDATED VERSION IN COSTAR_prep.ipynb!!!!!
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

def parcelBuildings(parcel,buildings):
    parcel.drop_duplicates(subset=['geometry'], inplace=True)
    parcel = sumWithin(parcel,buildings)
    parcel['geometry'] = parcel['geometry'].simplify(1)
    parcel['intLen'] = parcel.apply(lambda row: interiorLen(row.geometry), axis=1)
    parcel['intZscore'] = np.abs(stats.zscore(parcel['intLen']))
    parcel.drop(parcel[parcel.intLen >= 20].index, inplace=True) #dropping outlier inner geometries
    parcel.reset_index(inplace=True)
    parcel['extLen1'] = parcel.apply(lambda row: exteriorLen(row.geometry), axis=1)
    parcel['extZscore1'] = np.abs(stats.zscore(parcel['extLen1']))
    columns = ['APN', 'APN2', 'OWNER', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    return parcel

   
def parcelPreFilter(parcel):   
    parcel = parcel.drop(parcel[(parcel['extZscore1'] > 3) & (parcel['Sum_Within'] < 10)].index) #dropping outlier geometries
    parcel.drop(parcel[parcel['Sum_Within'] < new_func()].index, inplace=True) #change to 20 or 30 later
    parcel.drop(parcel[parcel['mnBlgArea'] > 175].index, inplace=True)
    parcel.reset_index(inplace=True)
    columns = ['APN', 'APN2', 'OWNER', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    return parcel

def new_func():
    return 5


def nearSelect(parcel, mobileHomes):
    cols = parcel.columns
    parcel['tempID'] = parcel.index
    mhps_parcels_near = gpd.sjoin_nearest(mobileHomes,parcel,max_distance=50, distance_col='distances')
    mhps_parcels_near.drop(cols, axis=1, inplace=True)
    merged = parcel.merge(mhps_parcels_near, on='tempID')
    merged.drop(['index_right'], axis=1, inplace=True)
    merged = merged.sort_values(['tempID','distances']).drop_duplicates(subset=['tempID'], keep='first')
    parcel['IDcheck'] = parcel['tempID'].isin(merged['tempID'].to_list())
    unmatched = parcel.loc[parcel['IDcheck']==False]
    secondJoin = gpd.sjoin_nearest(unmatched, mobileHomes, max_distance=50.0, distance_col='distances')
    secondJoin.drop('index_right',axis=1,inplace=True)
    concatted = pd.concat([merged,secondJoin])
    concatted.drop(['tempID','IDcheck'], axis=1, inplace=True)
    concatted['APN_JOIN'] = False
    cols = list(concatted.columns)
    cols.remove('geometry')
    cols.remove('distances')
    cols = cols + ['distances','geometry']
    concatted = concatted[cols]    
    return concatted

def apnJoin(parcel, mobileHomes):
    parcel['APN'] = parcel['APN'].str.replace('-','')
    mobileHomes['MH_APN'] = mobileHomes['MH_APN'].str.replace('-','')
    apnParcel = pd.merge(parcel,mobileHomes, left_on='APN', right_on='MH_APN').drop(columns={'geometry_y'})
    apnParcel.rename({'geometry_x':'geometry'}, axis='columns', inplace=True)
    apnParcel['APN_JOIN'] = True
    apnParcel['distances'] = float(-0.1)
    cols = list(apnParcel.columns)
    cols.remove('geometry')
    cols.append('geometry')
    apnParcel = apnParcel[cols]
    return apnParcel

def parcelMHPJoin(pFilePath):    
    fips = pFilePath.split('\\')[-1]
    mhpPath = os.path.join(pFilePath,fips+'_COSTAR_mhps.gpkg')
    with open(os.path.join(pFilePath,'exceptions.csv'),'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['COUNTY_FIPS','PROBLEM'])
        if os.path.exists(mhpPath):
            parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
            parcel.to_crs(crs='ESRI:102003', inplace=True)
            #buildings = gpd.read_file(os.path.join(pFilePath,fips+'_Buildings.gpkg'),layer=fips+'_Buildings')
            buildings = gpd.read_file(os.path.join(pFilePath,fips+'_buildings.shp'))
            buildings.to_crs(crs='ESRI:102003', inplace=True)
            mobileHomes = gpd.read_file(mhpPath)
            mobileHomes.to_crs(crs='ESRI:102003', inplace=True)
            parcel = parcelBuildings(parcel,buildings)  
            parcel = parcelPreFilter(parcel)
            apnParcel = apnJoin(parcel,mobileHomes)
            phomes = nearSelect(parcel,mobileHomes)
            phomes = pd.concat([apnParcel,phomes])
            phomesNullAPNs = phomes.loc[phomes['APN'].str.len() < 2]
            phomes = phomes.sort_values(by=['APN','APN_JOIN'], ascending=False).drop_duplicates(subset=['APN'], keep='first')
            phomes = pd.concat([phomes,phomesNullAPNs])
            phomes.drop_duplicates(subset=['geometry'], inplace=True)
            phomes.reset_index(inplace=True)
            phomes.drop(columns={'index'},inplace=True)            
            phomes = unitFilter(phomes)

            if len(phomes) > 0:
                phomes.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer='MH_parcels')
            else:
                writer.writerow([fips,'No MHP-Parcel Joins'])
        else:
            writer.writerow([fips,'NO MHP'])


def union_intersect(pFilePath):
    fips = pFilePath.split('\\')[-1]
    with open(os.path.join(pFilePath,'exceptions.csv'),'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if os.path.exists(os.path.join(pFilePath,fips+'.gpkg')) == True:
            phomes = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='MH_parcels')
            layers = fiona.listlayers(os.path.join(pFilePath,fips+'.gpkg'))
            for layer in layers:
                year = layer[-2:]
                blocks = gpd.read_file(os.path.join(pFilePath,f'{fips}_blocks.gpkg'), layer=layer)
                #blocks.to_crs(crs='ESRI:102003', inplace=True)
                blocks['blockArea_m'] = blocks['geometry'].area
                union = blocks.overlay(phomes, how='intersection')
                union['unionArea_m'] = union['geometry'].area
                union['blockParcel_ratio'] = (union['unionArea_m']/union['blockArea_m']) *100
                if len(union) > 0:
                    union.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer=f'MH_parc_blk_union20{year}')
                    union.to_csv(os.path.join(pFilePath,f'union_csv20{year}.csv'))
                else:
                    writer.writerow([fips, 'Join worked-but union missed-investigate'])
            

# need to check if all output columns are carried through

def mhp_union_merge(pFilePath):
    fips = pFilePath.split('\\')[-1]
    if os.path.exists(os.path.join(pFilePath,'MHP_'+ fips +'_COSTAR.csv')):
        mhp = pd.read_csv(os.path.join(pFilePath,'MHP_'+ fips +'_COSTAR.csv'), dtype={'MH_COUNTY_FIPS':str, 'MH_APN':str})
        years = ['00','10']
        for year in years:
            if os.path.exists(os.path.join(pFilePath,f'union_csv20{year}.csv')) == True:
                union = pd.read_csv(os.path.join(pFilePath,f'union_csv20{year}.csv'), dtype={f'GEOID{year}':str,f'STATEFP{year}':str, f'COUNTYFP{year}':str, f'TRACTCE{year}':str,f'BLOCKCE{year}':str, 'MH_APN':str})
                try:
                    mhp_union_merge = mhp.merge(union, on='MH_APN', how = 'outer')
                except:
                    print(fips)
                    pass
                #mhp_union_merge.drop(['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1, inplace=True)
                mhp_union_merge.drop(mhp_union_merge.filter(regex='_y$').columns, axis=1, inplace=True)
                renames_x = mhp_union_merge.filter(regex='_x$').columns
                renames = [x.split('_x')[0] for x in renames_x]
                renames = dict(zip(renames_x,renames))
                mhp_union_merge.rename(renames, axis='columns',inplace=True)
                mhp_union_merge.drop(mhp_union_merge.filter(regex='Unnamed*').columns,axis=1, inplace=True)
                mhp_union_merge.to_csv(os.path.join(pFilePath, f'MHP_{fips}_20{year}.csv'))
            else:
                mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
                mhp.to_csv(os.path.join(pFilePath, f'MHP_{fips}_20{year}.csv'))
