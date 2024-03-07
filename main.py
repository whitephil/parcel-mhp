if __name__ == '__main__':
    import time
    start = time.time()

    from multiprocessing import Pool
    import pandas as pd
    import geopandas as gpd
    import parcelfunks
    import os
    import pyogrio
    import warnings
    import winsound

    warnings.filterwarnings("ignore")
    #stateFips = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\stateFips.csv', dtype={'STATEFP':str})
    #stateFipsList = stateFips['STATEFP'].tolist()
    externalDrive = r'E:/'
    state = '08'
    
    stateDir = rf'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\CO_2022' #change later to {state}
    outDir = rf'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\{state}_output'
    if os.path.exists(rf'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\{state}_output') == False:
        os.mkdir(outDir)

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

    # primary workflow parallel tasks:
    with Pool() as pool:
        pool.map(parcelfunks.parcelWorker,parcelsPaths)

    print('Parcel-MHP Join Time:',time.time()-ti)

    ti = time.time()
    
    with Pool() as pool:
        pool.map(parcelfunks.unionWorker,parcelsPaths)
        pool.map(parcelfunks.mergeWorker,parcelsPaths)
    print('Union and Merge Steps Time:',time.time()-ti)
    
    # outputs:
    versions = {'COSTAR':{'_parcels',
                        '_parc_blk_union2000',
                        '_parc_blk_union2010'}, 
                'MHP':{'_parcels',
                        '_parc_blk_union2000',
                        '_parc_blk_union2010'}}
    years = ['2000','2010']

    for version in versions.keys():    
        for year in years:
            yr = year[-2:]
            outDF = pd.DataFrame()      
            for path in parcelsPaths:
                fips = path.split('\\')[-1]                
                dtype={f'STATEFP{yr}':str,f'COUNTYFP{yr}':str,f'TRACTCE{yr}':str,f'BLOCKCE{yr}':str,f'GEOID{yr}':str,f'MTFCC{yr}':str,f'UACE{yr}':str,f'GEOID{yr}':str, 'MH_COUNTY_FIPS':str}
                if version == 'COSTAR':
                    dtype.update({'MH_APN':str, 'APN':str, 'APN2':str})            
                filePath = os.path.join(path,f'{version}_{fips}_{year}.csv')            
                if os.path.exists(filePath):
                    unionDF = pd.read_csv(filePath,dtype=dtype)
                    outDF = pd.concat([outDF,unionDF])
            outDF.drop(outDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
            outDF.to_csv(os.path.join(outDir, f'{state}_{version}_{year}_final.csv'))

    exceptionsFinalDF = pd.DataFrame()
    for path in parcelsPaths:
        fips = path.split('\\')[-1]
        countyEx = pd.read_csv(os.path.join(path,'exceptions.csv'), dtype={'COUNTY_FIPS':str})
        exceptionsFinalDF = pd.concat([exceptionsFinalDF,countyEx])
    exceptionsFinalDF.to_csv(os.path.join(outDir, f'{state}_exceptions.csv'))

    # combine outputs into final state geodataframe    
    for key, values in versions.items():
        for v in values:
            outDF = pd.DataFrame()
            for path in parcelsPaths:
                fips = path.split('\\')[-1]        
                if os.path.exists(os.path.join(path,fips+'.gpkg')):
                    layers = pyogrio.list_layers(os.path.join(path,fips+'.gpkg'))
                    if f'{key}{v}' in layers:
                        outDF = pd.concat([outDF,gpd.read_file(os.path.join(path, fips+'.gpkg'),layer=f'{key}{v}')])
            outDF.to_file(os.path.join(outDir,f'{state}_Final.gpkg'),layer=f'{key}{v}')



    #COSTAR_joinDF.to_csv(os.path.join(outDir, 'Colorado_Final_COSTAR.csv'))
    #MHP_joinDF.to_csv(os.path.join(outDir, 'Colorado_Final_MHP.csv'))   

    winsound.Beep(450, 1000)  
    print('Done.')
    print('Total time:', time.time()-start)

