import pandas as pd
import os
import datetime
import fsspec

import xarray as xr
def add_time_info(df,verbose=False):
    '''add start, stop and nt for all zstores in df'''
    starts = []; stops = []; nts = []; calendars = []; units = []; ttypes = []
    dz = df.copy()
    for index, row in df.iterrows():
        zstore = row.zstore
        ds = xr.open_zarr(fsspec.get_mapper(zstore),consolidated=True) 
        start = 'NA'
        start = 'NA'
        nt = '1'
        if 'time' in ds.coords:
            ttype = str(type(ds.time.values[0]))
            #dstime = ds.time.values
            #start = str(dstime[0])[:10]
            #stop = str(dstime[-1])[:10]
            dstime = ds.time.values.astype('str')
            start = dstime[0][:10]
            stop = dstime[-1][:10]
            calendar = ds.time.encoding['calendar']
            unit = ds.time.encoding['units']
            nt = len(dstime)
            if verbose:
                print(zstore,start,stop,nt)
        starts += [start]
        stops += [stop]
        nts += [nt]
        calendars += [calendar]
        units += [unit]
        ttypes += [ttype]

    dz['start'] = starts
    dz['stop'] = stops
    dz['nt'] = nts
    dz['calendar'] = calendars
    dz['time_units'] = units
    dz['time_type'] = ttypes
    return dz

# define a simple search on keywords
def search_df(df, verbose= False, **search):
    '''search by keywords - if list, then match exactly, otherwise match as substring'''
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

from functools import partial
def getFolderSize(p):
    prepend = partial(os.path.join, p)
    return sum([(os.path.getsize(f) if os.path.isfile(f) else
                 getFolderSize(f)) for f in map(prepend, os.listdir(p))])

def get_zid(gsurl):
    ''' given a GC zarr location, return the dataset_id'''
    assert gsurl[:10] == 'gs://cmip6'
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
    '''delete old version in GC'''
    command = '/usr/bin/gsutil -m rm -r '+ gsurl[:-1]
    if execute:
        os.system(command) 
    else:
        print(command)
    return

def remove_from_GC_listing(gsurl,execute=False):
    '''delete entry in ncsv/GC_files_{activity_id}-{institution_id}.csv'''
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
        print('modifying ',file)
    return

from glob import glob 
def remove_from_drives(gsurl,execute=False):                
    '''delete old local copy(ies)'''
    gdirs = glob('/h*/naomi/zarr-minimal'+gsurl[10:])             
    if len(gdirs)==0:
        print('zstore is not on any mounted drives')
    else:        
        for gdir in gdirs:
            command = '/bin/rm -rf '+ gdir
            if execute:
                os.system(command)
            else:
                print(command)
    return

def remove_from_shelf(gsurl,execute=False):                
    '''delete entry(ies) in shelf-new/h*.csv'''
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
            print(zpath,f'dff.to_csv({file})')

        if not writeable:
            command = "chmod u-w " + file
            if execute:
                os.system(command)
    return 1

def remove_from_local_listings(gsurl,execute=False):                
    '''remove from concatenated catalog'''
    file = 'shelf-new/local.csv'
    df_local = pd.read_csv(file, dtype='unicode')
    
    for zpath in df_local[df_local.zstore.str.contains(gsurl[10:-1])].zstore.values:
        dff = df_local[df_local.zstore != zpath]
        if execute:
            dff.to_csv(file, mode='w+', index=False)
        else:
            print(f'dff.to_csv({file})')
    return    