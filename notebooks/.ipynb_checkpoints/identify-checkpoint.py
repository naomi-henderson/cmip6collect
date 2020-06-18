import warnings
import pandas as pd
import requests
import xarray as xr

def get_version(zstore):

    client = requests.session()
    baseurl =  'http://hdl.handle.net/api/handles/'
    query1 = '?type=IS_PART_OF'
    query2 = '?type=VERSION_NUMBER'

    tracking_ids = xr.open_zarr(zstore,consolidated=True).attrs['tracking_id']

    versions = []
    datasets = []
    for file_tracking_id in tracking_ids.split('\n')[0:1]:
        url = baseurl+file_tracking_id[4:]+query1
        r = client.get(url)
        r.raise_for_status()
        dataset_tracking_id = r.json()['values'][0]['data']['value']
        datasets += [dataset_tracking_id]
        if ';' in dataset_tracking_id:
            # multiple dataset_ids erroneously reported
            dtracks = dataset_tracking_id.split(';')
            vs = []
            for dtrack in dtracks:
                url2 = baseurl + dtrack[4:] + query2
                r = client.get(url2)
                r.raise_for_status()
                r.json()['values'][0]['data']['value']
                vs += [r.json()['values'][0]['data']['value']]
            v = sorted(vs)[-1]    
        else:
            url2 = baseurl + dataset_tracking_id[4:] + query2
            r = client.get(url2)
            r.raise_for_status()
            v = r.json()['values'][0]['data']['value']
        versions += [v]

    version_id = list(set(versions))
    dataset_id = list(set(datasets))

    assert len(version_id)==1
    
    return dataset_id[0], version_id[0]

def needed(dfm, df_req, dESGF):
    # Makes list of available zstores which are NOT in cloud 
    # dfm - df of data in cloud
    # df_req - request
    # dESGF - df of data available at ESGF matching request

    ngood = 0
    zarr_format = '/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/\
%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/'
    df_list = []

    for index, row in df_req.iterrows():

        timestamp = row['Timestamp']
        name = row['requester']
        email = row['E-mail']
        experiment_ids = row['experiments']
        source_ids = row['models']
        variable_ids = row['variables']
        member_ids = row['members']
        table_id = row['table']

        if len(member_ids[0]) == 0:
            member_ids = ['All']
            
        for variable_id in variable_ids:
            if experiment_ids == ['All']:
                experiments = dESGF[(dESGF.table_id==table_id)&(dESGF.variable_id==variable_id)].experiment_id.unique()
            else:
                experiments = experiment_ids

            for experiment_id in experiments:
                if source_ids == ['All']:
                    sources = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                               (dESGF.variable_id==variable_id)].source_id.unique()
                    #print(variable_id,experiment_id,sources)
                else:
                    sources = source_ids
                for source_id in sources:
                    df = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                            (dESGF.variable_id==variable_id)    &(dESGF.source_id==source_id)]
                    if len(df) == 0:
                        continue

                    members = df.member_id.unique()
           
                    if len(member_ids) == 1 :
                        first = member_ids[0]
                        if first == 'One':
                            members = [members[0]]
                    elif first != 'All':
                        if len(first) <=3 :
                                members = members[:int(member_ids[0])]
                        else:
                             members = [first]
                    elif len(member_ids) >= 2 :
                        members = member_ids
                       
                    for member_id in members:
                        
                        df_member = df[df.member_id==member_id]
                           
                        grid_labels = df_member.grid_label.unique()  
                        
                        for grid_label in grid_labels:
                            df_grid = df_member[df_member.grid_label==grid_label]
                            try:
                                file = df_grid.values[0]
                            except:
                                continue
 
                            #print(experiment_id,variable_id,source_id,member_id,grid_label)
                            
                            zarr_dir = dict(zip(df.keys(),file))
                            zarr_file = zarr_format % zarr_dir 

                            zstore = 'gs://cmip6' + zarr_file 
                        
                            #print(zstore)
                            df_cloud = dfm[(dfm.zstore==zstore)]
                            
                            if len(df_cloud) >= 1:
                                #print('store already in cloud')
                                continue
                            else:
                                ngood += 1

                            with warnings.catch_warnings():
                                warnings.filterwarnings("ignore")
                                df_grid.loc[:,'zstore'] = zarr_file

                            df_list += [df_grid]

    if ngood >= 1:
        return pd.concat(df_list)
    else: 
        #print('no new data available')
        return dESGF[dESGF.source_id=='junk']

def needed_newversion(dfm, df_req, dESGF):
    # Makes list of available zstores which are not in cloud OR have new versions available
    # dfm - df of data in cloud (version known)
    # df_req - request
    # dESGF - df of data available at ESGF matching request

    newv = 0
    zarr_format = '/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/\
%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/'
    df_list = []

    for index, row in df_req.iterrows():

        timestamp = row['Timestamp']
        name = row['requester']
        email = row['E-mail']
        experiment_ids = row['experiments']
        source_ids = row['models']
        variable_ids = row['variables']
        member_ids = row['members']
        table_id = row['table']

        if len(member_ids[0]) == 0:
            member_ids = ['All']
            
        for variable_id in variable_ids:
            if experiment_ids == ['All']:
                experiments = dESGF[(dESGF.table_id==table_id)&(dESGF.variable_id==variable_id)].experiment_id.unique()
            else:
                experiments = experiment_ids

            for experiment_id in experiments:
                if source_ids == ['All']:
                    sources = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                               (dESGF.variable_id==variable_id)].source_id.unique()
                    #print(variable_id,experiment_id,sources)
                else:
                    sources = source_ids
                for source_id in sources:
                    df = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                            (dESGF.variable_id==variable_id)    &(dESGF.source_id==source_id)]
                    if len(df) == 0:
                        continue

                    members = df.member_id.unique()
           
                    if len(member_ids) == 1 :
                        first = member_ids[0]
                        if first == 'One':
                            members = [members[0]]
                    elif first != 'All':
                        if len(first) <=3 :
                                members = members[:int(member_ids[0])]
                        else:
                             members = [first]
                    elif len(member_ids) >= 2 :
                        members = member_ids
                       
                    for member_id in members:
                        
                        df_member = df[df.member_id==member_id]
                           
                        grid_labels = df_member.grid_label.unique()  
                        
                        for grid_label in grid_labels:
                            df_grid = df_member[df_member.grid_label==grid_label]
                            try:
                                file = df_grid.values[0]
                            except:
                                continue
 
                            #print(experiment_id,variable_id,source_id,member_id,grid_label)
                            
                            if len(df_grid.version.unique()) > 1:
                               print(df_grid.version.unique())
                               continue
                            else:
                                version1 = df_grid.version.values[0][1:]
                    
                            zarr_dir = dict(zip(df.keys(),file))
                            zarr_file = zarr_format % zarr_dir 
                            zstore = 'gs://cmip6' + zarr_file 
                        
                            df_cloud = dfm[dfm.zstore==zstore]
                            
                            if len(df_cloud) >= 1:
                                if len(df_cloud.version.unique()) > 1:
                                   print(df_cloud.version.unique())
                                   continue
                                else:
                                    version2 = df_cloud.version.values[0]

                                if version1 == version2:  
                                    #print('same version already in cloud')
                                    continue
                                else:
                                    print('new version available',zstore,'new: ',version1,'old: ',version2)
                                    newv += 1
                            else:
                                #print('zstore not in cloud yet')
                                continue

                            with warnings.catch_warnings():
                                warnings.filterwarnings("ignore")
                                df_grid.loc[:,'zstore'] = zarr_file

                            df_list += [df_grid]

    if newv >= 1:
        return pd.concat(df_list)
    else: 
        #print('no new data available')
        return dESGF[dESGF.source_id=='junk']
