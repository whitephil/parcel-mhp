if __name__ == '__main__':
    import time
    start = time.time()

    from multiprocessing import Pool
    from glob import glob
    import pandas as pd
    import parcelfunks
    import os
    import warnings
    import winsound

    warnings.filterwarnings("ignore")
    #stateFips = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\stateFips.csv', dtype={'STATEFP':str})
    #stateFipsList = stateFips['STATEFP'].tolist()
    externalDrive = r'E:/'
    outDir = r'C:\Users\phwh9568\Data\ParcelAtlas'
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
    parcelsPaths = glob(r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties\*')

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
    
    stateFinalDF = pd.DataFrame()
    for path in parcelsPaths:
        if os.path.exists(os.path.join(path,'MHP_'+path.split('\\')[-1]+'.csv')):
            countyDF = pd.read_csv(os.path.join(path,'MHP_'+path.split('\\')[-1]+'_final.csv'), dtype={'STATEFP10':str,'COUNTYFP10':str,'TRACTCE10':str,'BLOCKCE10':str,'GEOID10':str,'MTFCC10':str,'UACE10':str,'GEOID10':str, 'MH_COUNTY_FIPS':str, 'MHPID':str})
            stateFinalDF = pd.concat([stateFinalDF,countyDF])
            
    stateFinalDF.drop(stateFinalDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
    #stateFinalDF.to_csv(os.path.join(externalDrive,f'State_{state}',f'{state}_final.csv'))
    stateFinalDF.to_csv(os.path.join(CO_path,f'{state}_final.csv'))

    winsound.Beep(450, 1000)  
    print('Done.')
    print('Total time:', time.time()-start)

