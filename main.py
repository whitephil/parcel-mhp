if __name__ == '__main__':
    import time
    start = time.time()

    from multiprocessing import Pool
    import pandas as pd
    import geopandas as gpd
    import parcelfunks
    import pyogrio
    import os
    import warnings
    import winsound

    #warnings.filterwarnings("ignore")
    #stateFips = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\stateFips.csv', dtype={'STATEFP':str})
    #stateFipsList = stateFips['STATEFP'].tolist()
    externalDrive = r'E:/'
    outDir = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022'
    
    # THIS INVENTORY FILE IS WRONG!!! or not quite right? Need to rerun w/ updated data
    '''
    pInventory = pd.read_csv(os.path.join(externalDrive,'parcelInventory.csv'), dtype={'STATE':str,'COUNTY':str})
    stateFips = list(pInventory['STATE'].unique())
    for state in stateFips:
        if state == '44':
            stateDir = os.path.join(externalDrive,f'State_{state}')
            stateInventory = pInventory.loc[pInventory['STATE']==state]
            stateInventory_True = stateInventory.loc[stateInventory['DATA_PRESENT']==True]
            stateInventory_False = stateInventory.loc[stateInventory['DATA_PRESENT']==False]
            stateInventory_False.to_csv(os.path.join(stateDir,f'{state}_missingParcelData.csv')) 
            fipsList = stateInventory_True['COUNTY'].tolist()
            parcelsPaths = [os.path.join(stateDir,fips) for fips in fipsList]
    '''
            # perhaps set up for loop first over stateFipsList here to form all paths? Or maybe run one state at a time.
            
    # Colorado:    
    state = '08'
    CO_path = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties'
    #CO_pInventory = pInventory.loc[pInventory['STATE']=='08']
    CO_pInventory = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\counties\ColoradoInventory.csv', dtype={'STATE':str,'COUNTY':str})
    CO_pInventory_True = CO_pInventory.loc[CO_pInventory['DATA_PRESENT']==True]
    CO_pInventory_False = CO_pInventory.loc[CO_pInventory['DATA_PRESENT']==False]
    CO_pInventory_False.to_csv(os.path.join(CO_path,'missingParcelData.csv')) # need to get this to work
    fipsList = CO_pInventory_True['COUNTY'].tolist()   
    parcelsPaths = [os.path.join(CO_path,fips) for fips in fipsList]
    #parcelsPaths = glob(r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties\*')

    ti = time.time()

    #original
    with Pool() as pool:
        pool.map(parcelfunks.parcelMHPJoin,parcelsPaths)

    print('Parcel-MHP Join Time:',time.time()-ti)
    

    ti = time.time()
    
    with Pool() as pool:
        pool.map(parcelfunks.union_intersect,parcelsPaths)
    
    print('MHParcels-Blocks Union Time:',time.time()-ti)
    

    ti = time.time()

    with Pool() as pool:
        pool.map(parcelfunks.mhp_union_merge,parcelsPaths)

    print('Table merge time:',time.time()-ti)
    
    years = ['2000','2010']
    for year in years:
        yr = year[-2:]
        stateFinalDF = pd.DataFrame()
        exceptionsFinalDF = pd.DataFrame()
        for path in parcelsPaths:
            fips = path.split('\\')[-1]
            countyEx = pd.read_csv(os.path.join(path,'exceptions.csv'))
            exceptionsFinalDF = pd.concat([exceptionsFinalDF,countyEx])
            if os.path.exists(os.path.join(path,f'MHP_{fips}_{year}.csv')):
                countyDF = pd.read_csv(os.path.join(path,f'MHP_{fips}_{year}.csv'), dtype={f'STATEFP{yr}':str,f'COUNTYFP{yr}':str,f'TRACTCE{yr}':str,f'BLOCKCE{yr}':str,f'GEOID{yr}':str,f'MTFCC{yr}':str,f'UACE{yr}':str,f'GEOID{yr}':str, 'MH_COUNTY_FIPS':str, 'MH_APN':str})
                stateFinalDF = pd.concat([stateFinalDF,countyDF])                
        stateFinalDF.drop(stateFinalDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
        #stateFinalDF.to_csv(os.path.join(externalDrive,f'State_{state}',f'{state}_{year}_final.csv'))
        stateFinalDF.to_csv(os.path.join(CO_path,f'{state}_{year}_final.csv'))
        exceptionsFinalDF.to_csv(os.path.join(outDir,'exceptions.csv'))

    #co_22_dir = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022'
        
        # need to iterate through fiona layers list do do this correctly, and perhaps abovegit 
    
    unionDF00 = pd.DataFrame()
    unionDF10 = pd.DataFrame()
    joinDF = pd.DataFrame()
    for path in parcelsPaths:
        fips = path.split('\\')[-1]        
        if os.path.exists(os.path.join(path,fips+'.gpkg')):
            layers = pyogrio.list_layers(os.path.join(path,fips+'.gpkg'))
            if 'MH_parc_blk_union2000' in layers:
                union00 = gpd.read_file(os.path.join(path, fips+'.gpkg'),layer='MH_parc_blk_union2000')
                unionDF00 = pd.concat([unionDF00,union00])
            if 'MH_parc_blk_union2010' in layers:
                union10 = gpd.read_file(os.path.join(path, fips+'.gpkg'),layer='MH_parc_blk_union2010')
                unionDF10 = pd.concat([unionDF10,union10])                
            join = gpd.read_file(os.path.join(path, fips+'.gpkg'),layer='MH_parcels')
            joinDF = pd.concat([joinDF,join])        
    unionDF00.to_file(os.path.join(outDir,'Colorado_Final.gpkg'),layer='Colorado_Final_union2000')
    unionDF10.to_file(os.path.join(outDir,'Colorado_Final.gpkg'),layer='Colorado_Final_union2010')

    #joinDF['polyLen_3'] = joinDF.apply(lambda row: geomLen(row.geometry), axis=1)
    #joinDF['geomZscore_3'] = np.abs(stats.zscore(joinDF['polyLen2']))
    joinDF.to_file(os.path.join(outDir, 'Colorado_Final.gpkg'),layer='Colorado_Final_MH_parcels')
    joinDF.to_csv(os.path.join(outDir, 'Colorado_Final.csv'))


    winsound.Beep(450, 1000)  
    print('Done.')
    print('Total time:', time.time()-start)

