import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
from ast import literal_eval
import os
import xarray as xr
import numpy as np


def set_bnds_as_coords(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    return ds

def read_codes(zarr):
    dex = pd.read_csv('csv/exceptions.csv',skipinitialspace=True)
    codes = []
    [source_id,experiment_id,member_id,table_id,variable_id,grid_label] = zarr.split('/')
    for ex in dex.values:
        dd = dict(zip(dex.keys(),ex))
        if dd['source_id'] == source_id or dd['source_id'] == 'all':
            if dd['experiment_id'] == experiment_id or dd['experiment_id'] == 'all':
                if dd['member_id'] == member_id or dd['member_id'] == 'all':
                    if dd['table_id'] == table_id or dd['table_id'] == 'all':
                        if dd['variable_id'] == variable_id or dd['variable_id'] == 'all':
                            if dd['grid_label'] == grid_label or dd['grid_label'] == 'all':                                 
                                codes += [dd['reason_code']]
                                print('special treatment needed:',dd['reason_code'])
    return codes

def get_ncfiles(zarr,df,skip_sites):
    # download any files needed for this zarr store (or abort the attempt)
    tmp = 'nctemp'
    
    okay = True
    files = df[df.zstore == zarr].file_name.unique()
    gfiles = []
    #urls = []
    for file in files:
        if okay:
            save_file = tmp + '/'+file
            expected_size = df[df.file_name == file]['size'].values[0]
            if os.path.isfile(save_file):
                if abs(os.path.getsize(save_file) - expected_size) <= 1000 :
                    #print('already have: ',save_file)
                    gfiles += [save_file]
                    continue

            url = df[df.file_name == file].HTTPServer_url.values[0]
            
            for site in skip_sites:
                if site in url:
                    #print('skip ',site,'domain for now')
                    trouble[zarr] = 'skipping ' + site + ' domain'
                    okay = False
            
            if not okay:
                continue
                
            command = 'curl ' + url + ' -o ' + save_file
            #print(command)
            os.system(command)

            if os.path.getsize(save_file) != expected_size:
                #print('trying curl command again')
                os.system(command)
                if os.path.getsize(save_file) != expected_size:
                    #print('second download did not fix issue - skipping file:',file)
                    trouble[zarr] = 'netcdf download not complete'
                    okay = False
            if os.path.getsize(save_file) == 0:
                os.system("rm -f "+save_file)
            if okay:
                gfiles += [save_file]
    return gfiles

def getsheet(json_keyfile,sheet_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)

    gc = gspread.authorize(credentials)

    wks = gc.open(sheet_name).sheet1

    data = wks.get_all_values()
    headers = data.pop(0)

    df = pd.DataFrame(data, columns=headers)

    df['experiments'] = [s.replace('*','').replace(' ','').split(',') for s in df.experiment_ids.values]
    df['models'] = [s.replace('All Available','All').replace(' ','').split(',') for s in df.source_ids.values]
    df['variables'] = [s.replace(' ','').split(',') for s in df['variable_ids (comma separated list)'].values]
    df['table'] = [s.replace(' ','').split(':')[0] for s in df.table_id.values]
    df['requester'] = df['Your name'] 

    df = df.drop(['Your name', 'Science Question/Motivation','Have you verified the existence of the data you will request?',
                  'table_id', 'source_ids', 'experiment_ids','variable_ids (comma separated list)', 'Questions and comments'],1)
    
    return df

def requests(): 
    json_keyfile = '/home/nhn2/json/Pangeo Hackathon-e48a41b13c91.json'
    sheet_name = "CMIP6 Hackathon Data Request (Responses)"

    df_all = getsheet(json_keyfile, sheet_name)
    df_all.to_csv('csv/dummy.csv',index=False)
    df_all = pd.read_csv('csv/dummy.csv')
      
    df_prior = pd.read_csv('csv/requests.csv')
    
    df_new = df_all.merge(df_prior, how='left', indicator=True)
    df_new = df_new[df_new['_merge']=='left_only'].drop('_merge',1)

    # convert strings back to lists
    for key in ['experiments','models','variables']:
        df_new.loc[:,key] = df_new.loc[:,key].apply(literal_eval)
    
    request_clean(df_new)
    
    return df_new

def set_request_id():
    return datetime.now().strftime('%Y%m-%d%H-%M%S')

def request_clean(df):
    experiments = list(pd.read_csv('csv/Experiments_tier1.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier2.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier3.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier4.csv').experiment_id.unique())
    
    df_var = pd.read_csv('csv/Variables.csv')
    variables = list(df_var.variable_id.unique())
    
    trouble_list = {}
    for item,row in df.iterrows():
        troubles = []
        email = row['E-mail']
        table_id = row['table']  
        
        experiment_ids = row['experiments']
        for experiment_id in experiment_ids:
            if not (experiment_id in experiments):
                troubles += [f"Warning: {experiment_id} is not a valid experiment_id"]
                
        variable_ids = row['variables']
        for variable_id in variable_ids:

            if variable_id in variables:
                tables = list(df_var[df_var.variable_id == variable_id].table_id.unique())
                #print(variable_id,tables)
                if not (table_id in tables):
                    troubles += [f"Warning: variable_id={variable_id} is not available in table_id={table_id}"]
        trouble_list[email+'_'+str(item)] = troubles
    for key in trouble_list.keys():
        [email,row] = key.split('_')
        print(row, email, trouble_list[key])

    return trouble_list

def concatenate(zarr,gfiles):

    institution_id = zarr.split('/')[-7]
    v_short = zarr.split(institution_id+'/')[1]

    ddict = ''
    codes = read_codes(v_short)

    #print('checking ',item,'/',len(new_zarrs),zbdir,'codes:', codes)
    if 'noUse' in codes:
        ddict[zarr] = 'code is noUse'
        return 'failure', None, ddict

    # guess chunk size by looking a first file: (not always a good choice - e.g. cfc11)
    nc_size = os.path.getsize(gfiles[0])
    ds = xr.open_dataset(gfiles[0])
    svar = ds.variable_id
    nt = ds[svar].shape[0]

    chunksize_optimal = 5e7
    chunksize = max(int(nt*chunksize_optimal/nc_size),1)

    #print('nt:',nt,'netcdf size:', nc_size/1e6, 'Mb')
    #print('suggested chunksize:', chunksize)

    if 'time' in ds.coords:   # please use cftime - piControl cannot use datetime64
        df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords, data_vars='minimal', chunks={'time': chunksize},
                                use_cftime=True, combine='nested', concat_dim='time') # combine='nested'
    else: # fixed in time, no time grid
        df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords, combine='by_coords', data_vars='minimal')

    for code in codes:
        if 'drop_tb' in code: # to_zarr cannot do chunking with time_bounds/time_bnds which is cftime (an object, not float)
            timeb = [var for var in df7.coords if 'time_bnds' in var or 'time_bounds' in var][0]
            df7 = df7.drop(timeb)
        if 'time_' in code:
            [y1,y2] = code.split('_')[-1].split('-')
            df7 = df7.sel(time=slice(str(y1)+'-01-01',str(y2)+'-12-31'))
        if '360_day' in code:
            year = gfiles[0].split('-')[-2][-6:-2]
            df7['time'] = cftime.num2date(np.arange(df7.time.shape[0]), units='months since '+year+'-01-16', calendar='360_day')
            #print('encoding time as 360_day from year = ',year)
        if 'noleap' in code:
            year = gfiles[0].split('-')[-2][-6:-2]
            df7['time'] = xr.cftime_range(start=year, periods=df7.time.shape[0], freq='MS', calendar='noleap').shift(15, 'D')
            #print('encoding time as noleap from year = ',year)
        if 'missing' in code:
            del df7[svar].encoding['missing_value']

    #     check time grid to make sure there are no gaps in concatenated data (open_mfdataset checks for mis-ordering)
    if 'time' in ds.coords:
        table_id = zarr.split('/')[-3]
        year = sorted(list(set(df7.time.dt.year.values)))
        #print(np.diff(year).sum(), len(year))
        if '3hr' in table_id:
            if not (np.diff(year).sum() == len(year)-1) | (np.diff(year).sum() == len(year)-2):
                ddict += '\n trouble with 3hr time grid'
                return 'failure', df7, ddict
        elif 'dec' in table_id:
            if not (np.diff(year).sum()/10 == len(year)) | (np.diff(year).sum()/10 == len(year)-1):
                ddict += '\ntrouble with dec time grid'
                return 'failure',df7, ddict
        else:
            if not np.diff(year).sum() == len(year)-1:
                ddict += '\ntrouble with grid'
                return 'failure',df7, ddict

    dsl = xr.open_dataset(gfiles[0])
    tracking_id = dsl.tracking_id
    if len(gfiles) > 1:
        for file in gfiles[1:]:
            dsl = xr.open_dataset(file)
            tracking_id = tracking_id+'\n'+dsl.tracking_id
    df7.attrs['tracking_id'] = tracking_id

    if 'time' in dsl.coords:
        df7 = df7.chunk(chunks={'time' : chunksize})   # yes, do it again

    return 'success', df7, ddict
