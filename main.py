if __name__ == '__main__':
    import time
    start = time.time()

    from multiprocessing import Pool
    from glob import glob
    import pandas as pd
    import parcelfunks
    import os

    stateFips = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\stateFips.csv', dtype={'STATEFP':str})
    stateFipsList = stateFips['STATEFP'].tolist()
    externalDrive = r'D:/'
    pInventory = pd.read_csv(os.path.join(externalDrive,'parcelInventory.csv'), dtype={'STATE':str,'COUNTY':str})

    # perhaps set up for loop first over stateFipsList here to form all paths? Or maybe run one state at a time.
    # Colorado:    
    CO_path = r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties'
    CO_pInventory = pInventory.loc[pInventory['STATE']=='08']
    CO_pInventory_True = CO_pInventory.loc[CO_pInventory['DATA_PRESENT']==True]
    CO_pInventory_False = CO_pInventory.loc[CO_pInventory['DATA_PRESENT']==False]
    CO_pInventory_False.to_csv(os.path.join(CO_path,'missingParcelData.csv'))
    fipsList = CO_pInventory_True['COUNTY'].tolist()   
    parcelsPaths = [os.path.join(CO_path,fips) for fips in fipsList]
    #parcelsPaths = glob(r'C:\Users\phwh9568\Data\ParcelAtlas\CO_2022\Counties\*')

    
    ti = time.time()

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
        if os.path.exists(os.path.join(path,'MHP_'+path.split('\\')[-1]+'_final.csv')):
            countyDF = pd.read_csv(os.path.join(path,'MHP_'+path.split('\\')[-1]+'_final.csv'), dtype={'STATEFP10':str,'COUNTYFP10':str,'TRACTCE10':str,'BLOCKCE10':str,'GEOID10':str,'MTFCC10':str,'UACE10':str,'GEOID10':str,'GEOID10':str, 'MH_COUNTY_FIPS':str, 'MHPID':str})
            stateFinalDF = pd.concat([stateFinalDF,countyDF])
            
    stateFinalDF.drop(stateFinalDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
    stateFinalDF.to_csv(r'c:/users/phwh9568/data/parcelatlas/CO_2022/Colorado.csv')
    
    print('Done.')
    print('Total time:', time.time()-start)

