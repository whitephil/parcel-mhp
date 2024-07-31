import os
import pyogrio
import geopandas as gpd
import pandas as pd
import csv
import numpy as np
from scipy import stats
import warnings

gpd.options.io_engine = "pyogrio"

warnings.filterwarnings('ignore')

# Mobile home data preparation funcs

def mhomes_prepper(mhomesPath):
    """
    Drops unwanted columns and renames columns in HIFLD mobile home dataset 
    for disambiguation purposes downstream.

    Parameters
    ----------
    mhomesPath: path to geodatabase containing the mobile home point data.

    Returns
    ----------
    Prepped mobile home point geodataframe.
    """
    mhomes = gpd.read_file(mhomesPath)
    mhomes.sindex
    columns = ['MHPID', 'NAME','ADDRESS', 'CITY', 'STATE', 'ZIP', 'STATUS', 'COUNTYFIPS', 'UNITS', 'SIZE', 'LATITUDE', 'LONGITUDE','geometry']
    renames = ['MHPID', 'MH_NAME','MH_ADDRESS', 'MH_CITY', 'MH_STATE', 'MH_ZIP', 'MH_STATUS','MH_COUNTY_FIPS', 'MH_UNITS', 'MH_SIZE', 'MH_LATITUDE', 'MH_LONGITUDE']
    drops = [c for c in mhomes.columns if c not in columns] 
    renames = dict(zip(columns,renames))
    mhomes.drop(drops,axis=1, inplace=True)
    mhomes.rename(renames, axis='columns',inplace=True)
    return mhomes


def costar_prepper(costarPath):
    """
    Same as mhomes_prepper but with COSTAR data. Drops unwanted 
    columns and renames columns in COSTAR mobile home dataset 
    for disambiguation purposes downstream. Adds county fips codes.

    Parameters
    ----------
    costarPath: path to geodatabase containing the COSTAR mobile home point data.
    fipsPath: path to csv containing fips codes for all US counties
    Returns
    ----------
    Prepped COSTAR mobile home point geodataframe.
    """
    #fipsCodes = pd.read_csv(fipsPath, dtype={'fips':str}, encoding='iso-8859-1')
    mhomes = gpd.read_file(costarPath, layer='COSTAR_mhps')
    mhomes.sindex
    columns = ['property_id', 'propertyname', 'property_address','propertycity','propertystate', 'propertycounty', 'fipscode', 'propertyzip5','latitude', 'longitude', 'costar_parcel','numberofunits', 'geometry']
    renames = ['MH_prop_id', 'MH_prop_name', 'MH_prop_add','MH_prop_city', 'MH_prop_state', 'MH_prop_county', 'MH_COUNTY_FIPS', 'MH_ZIP','MH_lat', 'MH_long', 'costar_parcel', 'MH_units']
    drops = [c for c in mhomes.columns if c not in columns] 
    renames = dict(zip(columns,renames))
    mhomes.drop(drops,axis=1, inplace=True)
    mhomes.rename(renames, axis='columns',inplace=True)
    #fipsCodes['MH_prop_county'] = fipsCodes['name'].str.replace(' County','')
    #mhomes = pd.merge(mhomes,fipsCodes[['MH_prop_county','fips']], on='MH_prop_county')
    #mhomes.rename({'fips':'MH_COUNTY_FIPS'}, axis='columns', inplace=True)
    return mhomes

def mhp_splitter(fips, MHPpath):
    """
    fips: fips code of the county being extracted
    MHPpath: path to gpkg of mhp data, costar or hifld
    
    """
    if 'COSTAR' in MHPpath:
        version = 'COSTAR'
    else:
        version = 'HIFLD'
    
    state = fips[0:2]
    countyPath = f'/scratch/alpine/phwh9568/State_{state}/{fips}'

    mhps = gpd.read_file(MHPpath, dtype={'MH_COUNTY_FIPS':str, 'MH_ZIP':str})
    mhps = mhps.loc[mhps['MH_COUNTY_FIPS']==fips]
    if len(mhps) > 0:
        mhps.to_file(os.path.join(countyPath,f'{fips}_{version}_mhps.gpkg'), driver='GPKG', layer=f'{version}_mhps')



# Parcel prefiltering functions

def interiorLen(geom):
    '''
    Sum of interior points in a polygon.
    Used downstream to filter out outlier geometries.

    Parameters
    ----------
    geom: a shapely polygon/multipolygon object from a geopandas geometry column. 

    Returns
    ----------
    Total number of interior points in the polygon object.
    '''
    if geom.geom_type == 'Polygon':
        return sum([len(g.xy[0]) for g in geom.interiors]) if len(geom.interiors) > 0 else 0
    if geom.geom_type == 'MultiPolygon':
        multiGeoms = geom.geoms
        return sum([sum([len(g.xy[0]) for g in mg.interiors]) if len(mg.interiors) > 0 else 0 for mg in multiGeoms])

def exteriorLen(geom):
    '''
    Sum of exterior points in a polygon. 
    Used downstream to filter out outlier geometries.

    Parameters
    ----------
    geom: a shapely polygon/multipolygon object from a geopandas geometry column. 

    Returns
    ----------
    Total number of exterior points in the polygon object.
    '''
    if geom.geom_type == 'Polygon':
        return len(geom.exterior.xy[0])
    if geom.geom_type == 'MultiPolygon':
        return sum([len(g.exterior.xy[0]) for g in geom.geoms])

def sumWithin(parcels,buildings):
    '''
    Sums the building footprints and aggregates the mean area of 
    buildings that intersect with a polygon. First uses geopandas.sjoin,
    to associate individual parcel with intersected buildings, then
    aggregates using pandas pivot_table and merges aggregate data to 
    the original parcel input. Bespoke, only works with this data... 
    but could be modified to be more flexible.

    This is a step in the prefiltering process. Used within parcelPreFilter function.

    Parameters
    ----------
    parcels: geodataframe of parcel data
    bulidings: geodataframe of building footprint data

    Returns
    ----------
    parcel polygons with counts of buildings within parcels 
    as well as mean building size within parcels.

    '''
    parcels['ID'] = parcels.index
    cols = parcels.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    parcels = parcels[cols]
    buildings['areas'] = buildings['geometry'].area
    dfsjoin = gpd.sjoin(parcels,buildings,predicate='intersects')
    print('dfsjoin len:', len(dfsjoin))
    dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FINDEX':len,'areas':'mean'}).reset_index()
    print('dfpivot len:',len(dfpivot))
    #dfpivot = pd.pivot_table(dfsjoin,index=['ID','APN'], aggfunc={'FID':len,'areas':'mean'}).reset_index()
    dfpolynew = parcels.merge(dfpivot, how='left', on='ID')
    if 'FINDEX' in dfpolynew: #remember to switch to 'FINDEX' when running in future
        dfpolynew.rename({'APN_x':'APN', 'FINDEX': 'Sum_Within', 'areas':'mnBlgArea'}, axis='columns',inplace=True)
        dfpolynew.fillna({'Sum_Within':0}, inplace=True)
        dfpolynew.fillna({'mnBlgArea':0}, inplace=True)
        dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)
    else:
        dfpolynew.rename({'APN_x':'APN'}, axis='columns',inplace=True)
        dfpolynew['Sum_Within'] = 0
        dfpolynew['mnBlgArea'] = 0
        dfpolynew.drop(['ID','APN_y'], axis=1, inplace=True)                
    return dfpolynew

def parcelPreFilter(parcel,buildings):
    '''
    -removes duplicated geometries (parcels stacked on one another)
    -removes None type geometries
    -counts buildings within parcels and calculates mean building size per parcel
    -simplifies geometries (which helps to standardize the frequency of vertexes in polygon)
    -Counts number of vertices in interior geometries and exterior geometries
    -Calculates z-score of lengths of interior and exterior geometries to find outlier parcels
    -(typically geometry features that are not real/legit parcels like a road, these are outliers)
    -drops outlier geometries
    -drops parcels with fewer than five buildings
    -drops parcels where mean building area is greater than 175 (too big to be mobile homes)
    
    Parameters
    ----------
    parcels: geodataframe of parcel data
    bulidings: geodataframe of building footprint data

    Returns
    ----------

    '''
    parcel.drop_duplicates(subset=['geometry'], inplace=True)
    parcel.drop(parcel[parcel['geometry']==None].index, inplace=True)
    print(len(parcel))
    print(parcel['APN'])
    parcel = sumWithin(parcel,buildings)
    parcel['geometry'] = parcel['geometry'].simplify(1.0)
    parcel['intLen'] = parcel.apply(lambda row: interiorLen(row.geometry), axis=1)
    parcel['intZscore'] = np.abs(stats.zscore(parcel['intLen']))
    parcel.drop(parcel[parcel.intLen >= 20].index, inplace=True)#dropping outlier inner geometries
    parcel.reset_index(inplace=True)
    parcel['extLen1'] = parcel.apply(lambda row: exteriorLen(row.geometry), axis=1)
    parcel['extZscore1'] = np.abs(stats.zscore(parcel['extLen1']))
    parcel.drop(parcel[parcel.extLen1 >= 1000].index, inplace=True)#dropping outlier exterior geometries
    parcel = parcel.drop(parcel[(parcel['extZscore1'] > 3) & (parcel['Sum_Within'] < 10)].index) #dropping outlier geometries
    print(len(parcel))
    print(len(parcel.loc[parcel['Sum_Within'] < 5]))
    parcel.drop(parcel[parcel['Sum_Within'] < 5].index, inplace=True) #change to 20 or 30 later? nevermind that
    print(len(parcel))
    parcel.drop(parcel[parcel['mnBlgArea'] > 175].index, inplace=True)
    print(len(parcel))
    parcel.reset_index(inplace=True)
    columns = ['APN', 'APN2', 'OWNER', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    return parcel

   
def parcelPreFilterOld(parcel):   
    parcel = parcel.drop(parcel[(parcel['extZscore1'] > 3) & (parcel['Sum_Within'] < 10)].index) #dropping outlier geometries
    parcel.drop(parcel[parcel['Sum_Within'] < 5].index, inplace=True) #change to 20 or 30 later? nevermind that
    parcel.drop(parcel[parcel['mnBlgArea'] > 175].index, inplace=True)
    parcel.reset_index(inplace=True)
    columns = ['APN', 'APN2', 'OWNER', 'intLen','intZscore', 'extLen1','extZscore1', 'Sum_Within', 'mnBlgArea', 'geometry']
    drops = [c for c in parcel.columns if c not in columns]
    parcel.drop(drops, axis=1, inplace=True)
    return parcel

# Parcel to Mobile Home point data joining functions

def apnJoin(parcel, mobileHomes):
    """
    Performs a pandas table merge based on common APN values. Standardizes
    APNs, merges, then creates boolean column for reach row indicating
    if join took place or not. Keeps unjoined parcels.

    Parameters
    ----------
    parcel: geodataframe of filtered parcel data
    mobileHomes: geodataframe of prepped COSTAR mobile home data set
    
    Returns
    ----------
    Geodataframe of parcels with merged coster mobile home data. 

    """
    parcel['APN'] = parcel['APN'].str.replace('-','')
    mobileHomes['MH_APN'] = mobileHomes['costar_parcel'].str.replace('-','')
    apnParcel = pd.merge(parcel,mobileHomes, left_on='APN', right_on='MH_APN').drop(columns={'geometry_y'})
    apnParcel.rename({'geometry_x':'geometry'}, axis='columns', inplace=True)
    apnParcel['APN_JOIN'] = True
    apnParcel['distances'] = float(-0.1)
    cols = list(apnParcel.columns)
    cols.remove('geometry')
    cols.append('geometry')
    apnParcel = apnParcel[cols]
    return apnParcel

def nearSelect(parcel, mobileHomes):
    """
    Performs two near-predicated spatial joins. First sjoin intends to
    get the closest available parcel, second gets other nearby potential
    mobile home parcels. There was a definite reason for this two-step
    process. Skips parcels joined via APN.
    
    Parameters
    ----------
    parcel: geodataframe of prepped and filtered parcel data
    mobileHomes: geodataframe of prepped mobile home data set

    Returns
    ----------
    Geodataframe with mobile home attributes joined.

    """
    cols = parcel.columns
    if 'tempID' in cols:
        cols = cols.drop('tempID')
    parcel['tempID'] = parcel.index
    mhps_parcels_near = gpd.sjoin_nearest(mobileHomes,parcel,max_distance=50, distance_col='distances')
    mhps_parcels_near.drop(cols, axis=1, inplace=True)
    merged = parcel.merge(mhps_parcels_near, on='tempID')
    merged.drop(['index_right'], axis=1, inplace=True)
    merged = merged.sort_values(['tempID','distances']).drop_duplicates(subset=['tempID'], keep='first')
    parcel['IDcheck'] = parcel['tempID'].isin(merged['tempID'].to_list())
    unmatched = parcel.loc[parcel['IDcheck']==False]
    secondJoin = gpd.sjoin_nearest(unmatched, mobileHomes, max_distance=100.0, distance_col='distances')
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

def parcelCostarJoin(parcel,costarHomes):
    """
    Worker function. Runs parcel-mhp join processes in sequence 
    over COSTAR version followed by unit filter.

    Parameters
    ----------
    parcel: geodataframe; prepped and filtered, and parcel data post-APN join attempt
    mobileHomes: geodataframe; prepped COSTAR mobile home data set

    Returns
    ----------
    Geodataframe phomes of parcel data with mobile home attributes, 
    with excess parcel-mhp polygons filtered out. 
    """

    apnParcel = apnJoin(parcel,costarHomes)
    len(apnParcel)
    phomes = nearSelect(parcel,costarHomes)
    phomes = pd.concat([apnParcel,phomes])
    #DROP duplicates if APN is null?
    phomesNullAPNs = phomes.loc[phomes['APN'].str.len() < 2]
    phomes = phomes.sort_values(by=['MH_prop_id','APN_JOIN'], ascending=False).drop_duplicates(subset=['MH_prop_id'], keep='first')
    phomes = pd.concat([phomes,phomesNullAPNs])
    #phomes.drop_duplicates(subset=['geometry'], inplace=True)
    phomes.sort_values(by=['APN'])
    phomes.reset_index(inplace=True)
    phomes.drop(columns={'index'},inplace=True)            
    phomes = unitFilter(phomes)
    return phomes

def parcelMHPJoin(parcel,mobileHomes):
    """
    Worker function. Runs parcel-mhp join processes in sequence 
    over HIFLD mobile home data followed by unit filter. Same as
    costarMHPJoin minus APN join process.

    Parameters
    ----------
    parcel: geodataframe; prepped and filtered, and parcel data 
    mobileHomes: geodataframe; prepped HIFLD mobile home data set

    Returns
    ----------
    Geodataframe phomes of parcel data with mobile home attributes, 
    with excess parcel-mhp polygons filtered out. 
    """
    phomes = nearSelect(parcel,mobileHomes)
    phomes.sort_values(by=['APN'])
    phomes.reset_index(inplace=True)
    phomes.drop(columns={'index'},inplace=True)  
    return phomes

def duplicateCheck(fips,phomes, writer):
    """
    Checks for duplicate parcels in the output. This looks for 
    a duplicate that occurs when two mobile home points intersect 
    the same parcel. These are not deduplicated, but noted in
    exceptions csv. 
    
    Parameters
    ----------
    fips:   str; fips code of current county
    phomes: geodataframe of parcel-mobile homes data post join processes
    writer: csv writer object from parcelWorker function
    """
    if len(phomes) > len(phomes['APN'].unique()):
        phomes['duplicated'] = phomes.duplicated(subset='APN', keep=False)
        dups = phomes.loc[phomes['duplicated'] == True]
        for i in range(len(dups)):            
            if len(dups.iloc[i]['APN']) > 2:
                if 'MH_prop_id' in dups.columns:
                    values = dups.iloc[i][['APN','MH_prop_id']].values
                    writer.writerow([fips,'DUPLICATE',f'APN:{values[0]}',f'propID:{values[1]}'])
                elif 'MHPID' in dups.columns:
                    values = dups.iloc[i][['APN','MHPID']].values
                    writer.writerow([fips,'DUPLICATE',f'APN:{values[0]}',f'MHPID:{values[1]}'])


# Post join quality assessment 

def rankNdrop(phomes):
    '''
    Unit filter step One.
    Rank and remove furthest parcel from point if building margin is high and positive
    This criteria would mean it is the parcel furthest and the number of buildings contained
    within this parcel pushes the total buildings within count beyond the building unit data reported in 
    original mhp data.
    '''
    phomes['rank'] = phomes.groupby('MH_APN')['distances'].rank(method='max')
    phomes.drop(phomes[(phomes['flag']==True) & (phomes['MH_Parcel_Count'] == phomes['rank'])].index, inplace=True)
    phomes.drop(columns={'MH_Parcel_Count', 'Total_Blds', 'BLD_UNIT_MARGIN', 'flag','rank'}, inplace=True)
    return(phomes)

def unitCheck(phomes):
    '''
    Unit filter step two. 
    first remove any duplicate parcel/distances (likely zero--a 0 dist from apn join 
    and wrong 0 dist from incorrect spatial join due to inaccurate geocode)
    phomes = phomes.sort_values(by=['MH_prop_id','distances','APN_JOIN']).drop_duplicates(subset=['distances','MH_prop_id'], keep='last')
    note that this drop based on duplicated 

    Flags any result where difference in sum within total of all parcels to mhp matches and input unit per MHP is greater than 15%
    '''
    piv = pd.pivot_table(phomes,index=['MH_APN','MH_units'], aggfunc={'Sum_Within':'sum', 'MH_prop_id':len}).reset_index()
    if len(piv) > 0:    
        piv.rename({'Sum_Within':'Total_Blds', 'MH_prop_id':'MH_Parcel_Count'},axis=1, inplace=True)
        piv['BLD_UNIT_MARGIN'] = (100 - ((piv['MH_units']/piv['Total_Blds'])*100))
        piv['flag'] = np.where((piv['MH_Parcel_Count'] > 1) & (piv['BLD_UNIT_MARGIN'] > 15),1,0) 
        phomes = pd.merge(phomes,piv[['MH_APN','MH_Parcel_Count','Total_Blds', 'BLD_UNIT_MARGIN', 'flag']],how='left', on='MH_APN')
    else:
        phomes = phomes.assign(MH_Parcel_Count =np.nan, Total_Blds=np.nan, BLD_UNIT_MARGIN=np.nan, flag=np.nan)
    return phomes

def unitFilter(phomes):
    '''
    Compares total sum of buildings summed within all parcels associated 
    with an MHP to the number of units reported in the MHP data. Helps to
    further verify results and remove false positive mhp-parcel associations.

    Iterates until all flagged mismatched mhp-parcels are removed.

    Parameters
    ----------
    parcels: geodataframe of parcel data

    Returns
    ----------
    parcel-homes geodataframe with incorrect matches removed based on unit check

    '''
    phomes = unitCheck(phomes)
    while 1 in phomes['flag'].values:
        phomes = rankNdrop(phomes)
        phomes = unitCheck(phomes)
    phomes.drop(columns={'flag'}, inplace=True)
    return(phomes)

# Post data join procedures: intersecting/union with blocks data and merging back with original mhp data

def union_intersect(pFilePath, fips, blocks, phomes, year, mhpVersion):
    """
    Runs through union overlay operation over parcel-mobile homes data
    and census block polygon data. Calculates proportion of unioned 
    mobile home parcels that overlap with a census block. Notes misses
    in exceptions csv. Writes union outputs to geopackage. Executed within
    unionWorker function.

    Parameters
    ----------
    pFilePath:  str; path to directory containing county level mhp-parcels gpkg
                and census blocks geopackage (used for writing output)
    fips:       str; fips code of the current county
    blocks:     geodataframe containing census block data
    phomes:     geodataframe mobile home park parcels output from merge/join process 
    year:       str; year of census block data (2000 or 2010)
    mhpVersion: str; version of mobile home park parcel data, either COSTAR of MHP
    """
    with open(os.path.join(pFilePath,'exceptions.csv'),'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        blocks['blockArea_m'] = blocks['geometry'].area
        union = blocks.overlay(phomes, how='intersection')
        union['unionArea_m'] = union['geometry'].area
        union['blockParcel_ratio'] = (union['unionArea_m']/union['blockArea_m']) *100
        if mhpVersion == 'COSTAR':
            if len(union) > 0:
                union.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer=f'COSTAR_parc_blk_union20{year}')
                union.to_csv(os.path.join(pFilePath,f'COSTAR_union_csv20{year}.csv'))
            else:
                writer.writerow([fips, 'COSTAR join worked-but union missed-investigate'])
        if mhpVersion == 'HIFLD':
            if len(union) > 0:
                union.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer=f'HIFLD_parc_blk_union20{year}')
                union.to_csv(os.path.join(pFilePath,f'HIFLD_union_csv20{year}.csv'))
            else:
                writer.writerow([fips, 'HIFLD join worked-but union missed-investigate'])


def mhp_union_merge(fips,version,pFilePath,versionName):
    """
    Merges all unioned mhp parcels/blocks data versions back with
    original version of mobile home data. Manages year and mobile
    home park version (COSTAR or HIFLD). Keeps unjoined mobile home
    parks in original dataset. After table merge, some column
    clean up. Writes outputs to csv.

    Parameters
    ----------
    fips:        str, fips code of current county
    version:     str, either 'COSTAR' of 'HIFLD'
    pFilePath:   str, path to county data
    versionName: str, costar or HIFLD plus fips (from mergeWorker)  

    """    
    years = ['00','10']
    for year in years:
        if version == 'COSTAR':
            mhp = pd.read_csv(os.path.join(pFilePath,versionName), dtype={'MH_COUNTY_FIPS':str, 'MH_APN':str})
            mhp['MH_APN'] = mhp['MH_APN'].str.replace('-','')
            dtype = {f'GEOID{year}':str,f'STATEFP{year}':str, f'COUNTYFP{year}':str, f'TRACTCE{year}':str,f'BLOCKCE{year}':str, 'MH_APN':str, 'APN':str, 'APN2':str}
        if version == 'HIFLD':
            mhp = pd.read_csv(os.path.join(pFilePath,versionName), dtype={'COUNTYFIPS':str, 'MHPID':str})
            dtype = {f'GEOID{year}':str,f'STATEFP{year}':str, f'COUNTYFP{year}':str, f'TRACTCE{year}':str,f'BLOCKCE{year}':str, 'MHPID':str}
        unionPath = os.path.join(pFilePath,f'{version}_union_csv20{year}.csv')
        if os.path.exists(unionPath):
            union = pd.read_csv(unionPath, dtype=dtype)
            if version == 'COSTAR':
                mhp_union = mhp.merge(union, on='MH_prop_id', how = 'outer')
            if version == 'HIFLD':
                mhp_union = mhp.merge(union, on='MHPID', how = 'outer')
            #mhp_union_merge.drop(['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1, inplace=True)
            mhp_union.drop(mhp_union.filter(regex='_y$').columns, axis=1, inplace=True)
            renames_x = mhp_union.filter(regex='_x$').columns
            renames = [x.split('_x')[0] for x in renames_x]
            renames = dict(zip(renames_x,renames))
            mhp_union.rename(renames, axis='columns',inplace=True)
            mhp_union.drop(mhp_union.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp_union.to_csv(os.path.join(pFilePath, f'{version}_{fips}_20{year}.csv'))
        else:
            mhp.drop(mhp.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            mhp.to_csv(os.path.join(pFilePath, f'{version}_{fips}_20{year}.csv'))

# Driver functions

def parcelWorker(pFilePath):
    """
    Step 1. Worker function executed in main script. Reads in parcel, 
    buildings, mhp datasets, runs all preprocesses and filters,
    runs join process, differentiates between COSTAR and HIFLD mobile
    home data. After running COSTAR version, runs all remaining
    potential mhp parcels through join process on HIFLD data. Notes
    missing, non-joined, and potential problem counties to 
    exceptions.csv; writes parcel-mhp matched polygons to geopackage.

    Parameters
    ----------
    pFilePath: str; file path to directory containing county level parcel data, 
               mobile home data, buildings data.
    
    """    
    pFilePath = pFilePath.strip()
    fips = pFilePath.split('/')[-1] #on linux should be '/'
    if fips[0:2] == '02':
        crs = 'EPSG:6393'
    elif fips[0:2] == '15':
        crs = 'ESRI:102007'
    else:
        crs = 'ESRI:102005'
    print(pFilePath)
    print(fips)
    costarPath = os.path.join(pFilePath,fips+'_COSTAR_mhps.gpkg')
    hifldPath = os.path.join(pFilePath,fips+'_HIFLD_mhps.gpkg')
    print(costarPath)
    print(hifldPath)
    with open(os.path.join(pFilePath,'exceptions.csv'),'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['COUNTY_FIPS','PROBLEM','NOTE_1','NOTE_2'])
        if os.path.exists(costarPath) or os.path.exists(hifldPath):
            if fips == '06037':
                parcel = gpd.read_file(os.path.join(pFilePath, f'{fips}_parcels.gpkg'), layer='parcels')
            elif fips == '17031':
                parcel = gpd.read_file(os.path.join(pFilePath, f'{fips}_parcels.gpkg'), layer='parcels')
            elif fips == '48201':
                parcel = gpd.read_file(os.path.join(pFilePath, f'{fips}_parcels.gpkg'), layer='parcels')
            else:
                try:
                    parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'))
                except Exception as e:
                    if type(e).__name__=='UnicodeDecodeError':
                        parcel = gpd.read_file(os.path.join(pFilePath,'parcels.shp'),encoding='iso-8859-1')
            print(fips)
            parcel.to_crs(crs=crs, inplace=True)
            if len(parcel['APN'].unique()) == 1:
                if parcel['APN'].unique() == None:
                    parcel['APN'] = np.random.randint(low=1, high=1000000000, size=len(parcel)).astype(str)
                    parcel['APN'] = 'F' + parcel['APN']
            buildings = gpd.read_file(os.path.join(pFilePath,fips+'_Buildings.gpkg'),layer=fips+'_Buildings')
            #buildings = gpd.read_file(os.path.join(pFilePath,fips+'_buildings.shp'))
            buildings.to_crs(crs=crs, inplace=True)
            parcel = parcelPreFilter(parcel,buildings)
            #parcel = parcelPreFilter(parcel)
            parcel['UNIQID'] = np.random.randint(low=1, high=1000000000, size=len(parcel))
            #run on COSTAR mhp data:
            if os.path.exists(costarPath):
                costarHomes = gpd.read_file(costarPath, layer='COSTAR_mhps')
                costarHomes.to_crs(crs=crs, inplace=True)
                costarParcels = parcelCostarJoin(parcel,costarHomes)
                duplicateCheck(fips,costarParcels, writer)
                costarParcels['COSTAR'] = 1
                #next three lines removing costar matches from original parcel dataset
                parcel = parcel.merge(costarParcels[['COSTAR', 'UNIQID']], how='left', on='UNIQID')
                parcel = parcel.loc[parcel['COSTAR']!=1]
                #parcel.drop(columns=['COSTAR','UNIQID'], inplace=True)
                if len(costarParcels) > 0:
                    costarParcels.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer='COSTAR_parcels')
                else:
                    writer.writerow([fips,'No COSTAR-Parcel Joins'])                
            else:
                writer.writerow([fips,'NO COSTAR MHPs'])
            #any remaining parcels try joining with HIFLD mhp dataset:
            if len(parcel) > 0:
                if os.path.exists(hifldPath):
                    mobileHomes = gpd.read_file(hifldPath, layer='HIFLD_mhps')
                    mobileHomes.to_crs(crs=crs, inplace=True)
                    mhpParcels = parcelMHPJoin(parcel,mobileHomes)
                    duplicateCheck(fips,mhpParcels,writer)
                    if len(mhpParcels) > 0:
                        mhpParcels.to_file(os.path.join(pFilePath,fips+'.gpkg'),driver='GPKG', layer='HIFLD_parcels')
                    else:
                        writer.writerow([fips,'No HIFLD-Parcel Joins'])
                else:
                    writer.writerow([fips,'NO HFLD MHPs'])
            else: writer.writerow([fips,'No remaining parcels post-costar join'])
        else:
            writer.writerow([fips,'NO COSTAR or HIFLDs'])



def unionWorker(pFilePath):
    """
    Step 2. Worker function executed in main script following mergeWorker. 
    Reads county census blocks, newly created mobile home parcel data, and 
    differentiates and manages 2000 vs 2010 blocks and COSTAR vs HIFLD mhp-parcels.

    Parameters
    ----------
    pFilePath: str; path to file directory of county level mobile home park parcels
               and census blocks data.

    """
    pFilePath = pFilePath.strip()
    fips = pFilePath.split('/')[-1]
    if os.path.exists(os.path.join(pFilePath,fips+'.gpkg')) == True:
        parcelLayers = pyogrio.list_layers(os.path.join(pFilePath,fips+'.gpkg'))            
        blockLayers = pyogrio.list_layers(os.path.join(pFilePath,fips+'_blocks.gpkg'))
        if fips[0:2] == '02':
            crs = 'EPSG:6393'
        elif fips[0:2] == '15':
            crs = 'ESRI:102007'
        else:
            crs = 'ESRI:102005'
        for blayer in blockLayers:
            year = blayer[0][-2:]
            blocks = gpd.read_file(os.path.join(pFilePath,f'{fips}_blocks.gpkg'), layer=blayer[0])
            blocks.to_crs(crs=crs, inplace=True)
            if 'COSTAR_parcels' in parcelLayers:
                mhpVersion = 'COSTAR'
                phomes = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='COSTAR_parcels')
                union_intersect(pFilePath, fips, blocks, phomes, year, mhpVersion)    
            if 'HIFLD_parcels' in parcelLayers:
                mhpVersion = 'HIFLD'
                phomes = gpd.read_file(os.path.join(pFilePath,fips+'.gpkg'), layer='HIFLD_parcels')
                union_intersect(pFilePath, fips, blocks, phomes, year, mhpVersion) 
                

def mergeWorker(pFilePath):
    """
    Step 3. Worker function executed in main script. Detects 
    version (COSTAR or HIFLD), manages file naming inputs. 
    Executes mhp_union_merge function.

    Parameters
    ----------
    pFilePath: file path to all county-level data layers
    """
    pFilePath = pFilePath.strip()
    fips = pFilePath.split('/')[-1]
    costarName = f'COSTAR_{fips}.csv'
    hifldName = f'HIFLD_{fips}.csv'
    if os.path.exists(os.path.join(pFilePath, costarName)):
        version = 'COSTAR'
        mhp_union_merge(fips,version,pFilePath,costarName)

    if os.path.exists(os.path.join(pFilePath,hifldName)):
        version = 'HIFLD'
        mhp_union_merge(fips,version,pFilePath,hifldName)        
