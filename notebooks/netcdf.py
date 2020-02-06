import os
import numpy as np
import pandas as pd
import xarray as xr
import datetime
import time

def set_bnds_as_coords(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    return ds

def set_bnds_as_coords_drop_height(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    if 'height' in ds.coords:
        ds = ds.drop('height')
    return ds

def get_ncfiles(zarr,df,skip_sites):
    # download any files needed for this zarr store (or abort the attempt)
    tmp = 'nctemp'
    
    institution_id = zarr.split('/')[-8]
    v_short = zarr.split(institution_id+'/')[1][:-1]

    codes = read_codes(v_short)
    if 'noUse' in codes:
        return [], 'noUse in codes',codes
    
    okay = True
    files = df[df.zstore == zarr].file_name.unique()

    gfiles = []
    trouble = ''
    for file in files:
        if okay:
            save_file = tmp + '/'+file
            expected_size = df[df.file_name == file]['size'].values[0]
            if os.path.isfile(save_file):
                if abs(os.path.getsize(save_file) - expected_size) <= 1000 :
                    gfiles += [save_file]
                    continue

            url = df[df.file_name == file].HTTPServer_url.values[0]
            
            for site in skip_sites:
                if site in url:
                    trouble += '\tskipping ' + site + ' domain'
                    okay = False
            
            if not okay:
                continue
                
            command = 'curl ' + url + ' -o ' + save_file
            print(command)
            os.system(command)
            time.sleep( 2 )

            if os.path.getsize(save_file) != expected_size:
                os.system(command)
                if os.path.getsize(save_file) != expected_size:
                    trouble += '\nnetcdf download not complete'
                    okay = False
            if os.path.getsize(save_file) == 0:
                os.system("rm -f "+save_file)
            if okay:
                gfiles += [save_file]
    return gfiles, trouble, codes

def concatenate(zarr,gfiles,codes):
    
    if len(codes) > 0:
        write_log(log_file,f'special treatment needed: {codes}',verbose=verbose)
        for code in codes:
            dstr += '\ncodes = ' + code

    # guess chunk size by looking a first file: (not always a good choice - e.g. cfc11)
    nc_size = os.path.getsize(gfiles[0])
    ds = xr.open_dataset(gfiles[0])
    svar = ds.variable_id
    nt = ds[svar].shape[0]

    chunksize_optimal = 5e7
    chunksize = max(int(nt*chunksize_optimal/nc_size),1)

    for code in codes:
        if 'deptht' in code:
            fix_string = '/usr/bin/ncrename -d .deptht\,olevel -v .deptht\,olevel -d .deptht_bounds\,olevel_bounds -v .deptht_bounds\,olevel_bounds '
            for gfile in gfiles:
                write_log(log_file,f'fixing deptht trouble in gfile:{gfile}',verbose=verbose)
                os.system(f'{fix_string} {gfile}')

    try:
        if 'time' in ds.coords:   
            df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords, data_vars='minimal', chunks={'time': chunksize},
                                    use_cftime=True, combine='nested', concat_dim='time') # combine='nested'
            
        else: # fixed in time, no time grid
            df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords, combine='by_coords', data_vars='minimal')
    except:
        print('trouble in open_mfdataset')
        dstr += '\nerror in open_mfdataset'

        if 'drop_height' in codes: 
            try:
                if 'time' in ds.coords:   
                    df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords_drop_height, data_vars='minimal', chunks={'time': chunksize},
                                        use_cftime=True, combine='nested', concat_dim='time') # combine='nested'
                else: # fixed in time, no time grid
                    df7 = xr.open_mfdataset(gfiles, preprocess=set_bnds_as_coords_drop_height, combine='by_coords', data_vars='minimal')
                    dstr += '\nerror fixed by drop_height'
            except:
                dstr += '\nerror not fixed by drop_height'
                return 'failure',ds, dstr                
        else:
            dstr += '\nerror not fixable'
            return 'failure',ds, dstr
                
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
        if 'fix_time' in code:
            df7['time'] = xr.cftime_range(start=df7.time[0].values.tolist(), periods=df7.time.shape[0], freq='D')
            #print('changing all times to standard day calendar')

    #     check time grid to make sure there are no gaps in concatenated data (open_mfdataset checks for mis-ordering)
    if 'time' in ds.coords:
        table_id = zarr.split('/')[-3]
        year = sorted(list(set(df7.time.dt.year.values)))
        #print(np.diff(year).sum(), len(year))
        if '3hr' in table_id:
            if not (np.diff(year).sum() == len(year)-1) | (np.diff(year).sum() == len(year)-2):
                dstr += '\ntrouble with 3hr time grid'
                return 'failure', df7, dstr
        elif 'dec' in table_id:
            if not (np.diff(year).sum()/10 == len(year)) | (np.diff(year).sum()/10 == len(year)-1):
                dstr += '\ntrouble with dec time grid'
                return 'failure',df7, dstr
        else:
            if not np.diff(year).sum() == len(year)-1:
                dstr += '\ntrouble with grid'
                return 'failure',df7, dstr

    dsl = xr.open_dataset(gfiles[0])
    tracking_id = dsl.tracking_id
    if len(gfiles) > 1:
        for file in gfiles[1:]:
            dsl = xr.open_dataset(file)
            tracking_id = tracking_id+'\n'+dsl.tracking_id
    df7.attrs['tracking_id'] = tracking_id

    date = str(datetime.datetime.now().strftime("%Y-%m-%d"))
    nstatus = date + ';created; by gcs.cmip6.ldeo@gmail.com'
    df7.attrs['status'] = nstatus
    
    if 'time' in dsl.coords:
        df7 = df7.chunk(chunks={'time' : chunksize})   # yes, do it again

    return 'success', df7, dstr

def read_codes(zarr):
    dex = pd.read_csv('csv/exceptions.csv',skipinitialspace=True)
    codes = []
    [source_id,experiment_id,member_id,table_id,variable_id,grid_label] = zarr.split('/')
    for ex in dex.values:
        dd = dict(zip(dex.keys(),ex))
        #print(source_id,experiment_id,member_id,table_id,variable_id,grid_label)
        if dd['source_id'] == source_id or dd['source_id'] == 'all':
            #print(source_id)
            if dd['experiment_id'] == experiment_id or dd['experiment_id'] == 'all':
                #print(experiment_id)
                if dd['member_id'] == member_id or dd['member_id'] == 'all':
                    #print(member_id)
                    if dd['table_id'] == table_id or dd['table_id'] == 'all':
                        #print(table_id)
                        if dd['variable_id'] == variable_id or dd['variable_id'] == 'all':
                            #print(variable_id)
                            if dd['grid_label'] == grid_label or dd['grid_label'] == 'all':  
                                #print(dd['reason_code'])
                                codes += [dd['reason_code']]
    #print(codes)                     
    return codes

