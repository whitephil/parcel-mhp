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
    '''
    def fxn():
        warnings.warn("deprecated", RuntimeWarning)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fxn()
    '''
    #stateFips = pd.read_csv(r'C:\Users\phwh9568\Data\ParcelAtlas\stateFips.csv', dtype={'STATEFP':str})
    #stateFipsList = stateFips['STATEFP'].tolist()
    #externalDrive = r'E:/'
    # THIS INVENTORY FILE IS WRONG!!!
    #pInventory = pd.read_csv(os.path.join(externalDrive,'parcelInventory.csv'), dtype={'STATE':str,'COUNTY':str})

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
        if os.path.exists(os.path.join(path,'MHP_'+path.split('\\')[-1]+'_double.csv')):
            countyDF = pd.read_csv(os.path.join(path,'MHP_'+path.split('\\')[-1]+'_double.csv'), dtype={'STATEFP10':str,'COUNTYFP10':str,'TRACTCE10':str,'BLOCKCE10':str,'GEOID10':str,'MTFCC10':str,'UACE10':str,'GEOID10':str,'GEOID10':str, 'MH_COUNTY_FIPS':str, 'MHPID':str})
            stateFinalDF = pd.concat([stateFinalDF,countyDF])
            
    stateFinalDF.drop(stateFinalDF.filter(regex='Unnamed*').columns,axis=1, inplace=True)
    stateFinalDF.to_csv(r'c:/users/phwh9568/data/parcelatlas/CO_2022/Colorado_Final_multi100_less5.csv')

    winsound.Beep(450, 1000)  
    print('Done.')
    print('Total time:', time.time()-start)

