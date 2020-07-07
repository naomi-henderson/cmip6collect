import pandas as pd
import os
import datetime

# define a simple search on keywords
def search_df(df, verbose= False, **search):
    "search by keywords - if list, then match exactly, otherwise match as substring"
    keys = ['activity_id','institution_id','source_id','experiment_id','member_id', 'table_id', 'variable_id', 'grid_label']
    d = df
    for skey in search.keys():
        
        if isinstance(search[skey], str):  # match a string as a substring
            d = d[d[skey].str.contains(search[skey])]
        else:
            dk = []
            for key in search[skey]:       # match a list of strings exactly
                dk += [d[d[skey]==key]]
            d = pd.concat(dk)
            keys.remove(skey)
    if verbose:
        for key in keys:
            print(key,' = ',list(d[key].unique()))      
    return d

def getFolderSize(p):
    prepend = partial(os.path.join, p)
    return sum([(os.path.getsize(f) if os.path.isfile(f) else
                 getFolderSize(f)) for f in map(prepend, os.listdir(p))])

def get_zid(gsurl):
    ''' given a GC zarr location, return the dataset_id'''
    return gsurl[11:-1].split('/')

def get_zdict(gsurl):
    ''' given a GC zarr location, return a dictionary of keywords'''
    zid = get_zid(gsurl)
    keys = ['activity_id','institution_id','source_id','experiment_id','member_id','table_id','variable_id','grid_label']
    values = list(zid)
    return dict(zip(keys,values)) 

def remove_from_GC(gsurl,execute=False):
    '''gsurl is a GC zstore, use execute=False to test, execute=True to remove'''
    remove_from_GC_bucket(gsurl,execute=execute)
    remove_from_GC_listing(gsurl,execute=execute)
    return

def remove_from_local(gsurl,execute=False):
    '''gsurl is a GC zstore, use execute=False to test, execute=True to remove'''  
    remove_from_drives(gsurl,execute=execute)
    ret = remove_from_shelf(gsurl,execute=execute)
    if ret==1:
        remove_from_local_listings(gsurl,execute=execute)
    else:
        print('zstore is not in any shelf listings')
    return

def remove_from_catalogs(gsurl,execute=False):
    '''gsurl is a GC zstore, use execute=False to test, execute=True to remove'''  
    date = str(datetime.datetime.now().strftime("%Y%m%d"))

    cat_files = ['csv/pangeo-cmip6-noQC']
    for cat_file in cat_files:
        os.system(f'cp {cat_file}.csv {cat_file}-'+date+'.csv')
        df = pd.read_csv(f'{cat_file}.csv', dtype='unicode')
        df = df[df['zstore'] != gsurl]
        df.to_csv(f'{cat_file}.csv', mode='w+', index=False)
    return
    
def remove_from_GC_bucket(gsurl,execute=False):
    ''' 1. delete old version in GC'''
    command = '/usr/bin/gsutil -m rm -r '+ gsurl[:-1]
    if execute:
        os.system(command) 
    else:
        print('# 1.',command)
    return

def remove_from_GC_listing(gsurl,execute=False):
    ''' 2. delete entry in ncsv/GC_files_{activity_id}-{institution_id}.csv'''
    zdict = get_zdict(gsurl)
    activity_id = zdict['activity_id']
    institution_id = zdict['institution_id']
    
    file = f'ncsv/GC_files_{activity_id}-{institution_id}.csv'
    if execute:
        with open(file, "r") as f:
            lines = f.readlines()
        with open(file, "w") as f:
            for line in lines:
                if line.strip("\n") != gsurl + ".zmetadata":
                    f.write(line)
    else:
        print('# 2.','modifying ',file)
    return

from glob import glob 
def remove_from_drives(gsurl,execute=False):                
    ''' 3. delete old local copy(ies)'''
    gdirs = glob('/h*/naomi/zarr-minimal'+gsurl[10:])             
    if len(gdirs)==0:
        print('# 3.','zstore is not on any mounted drives')
    else:        
        for gdir in gdirs:
            command = '/bin/rm -rf '+ gdir
            if execute:
                os.system(command)
            else:
                print('# 3.',command)
    return

def remove_from_shelf(gsurl,execute=False):                
    ''' 4. delete entry(ies) in shelf-new/h*.csv'''
    file = 'shelf-new/local.csv'
    df_local = pd.read_csv(file, dtype='unicode')
    zpaths = df_local[df_local.zstore.str.contains(gsurl[10:-1])].zstore.values
    if len(zpaths)==0:
        return 0
    for zpath in zpaths:
        #print('  ',zpath)
        ldir = zpath.split('/')[1]
        file = 'shelf-new/' + ldir + '.csv'
        writeable = os.access(file, os.W_OK)
        if not writeable:
            command = "chmod u+w " + file
            if execute:
                os.system(command)

        dfff = pd.read_csv(file, dtype='unicode')
        dff = dfff[dfff.zstore != zpath]
        if execute:
            dff.to_csv(file, mode='w+', index=False)
        else:
            print('# 4.',zpath,f'dff.to_csv({file})')

        if not writeable:
            command = "chmod u-w " + file
            if execute:
                os.system(command)
    return 1

def remove_from_local_listings(gsurl,execute=False):                
    ''' 5. remove from concatenated catalog'''
    file = 'shelf-new/local.csv'
    df_local = pd.read_csv(file, dtype='unicode')
    
    for zpath in df_local[df_local.zstore.str.contains(gsurl[10:-1])].zstore.values:
        dff = df_local[df_local.zstore != zpath]
        if execute:
            dff.to_csv(file, mode='w+', index=False)
        else:
            print('# 5.',f'dff.to_csv({file})')
    return    