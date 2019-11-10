import warnings
import pandas as pd

def needed(dfm, df_req, dESGF):

    ngood = 0
    zarr_format = '/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/\
%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s'
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

        for variable_id in variable_ids:
            if experiment_ids == ['All']:
                experiments = dESGF[(dESGF.table_id==table_id)&(dESGF.variable_id==variable_id)].experiment_id.unique()
            else:
                experiments = experiment_ids

            for experiment_id in experiments:
                if source_ids == ['All']:
                    sources = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                               (dESGF.variable_id==variable_id)].source_id.unique()
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
                        #print(experiment_id,variable_id,source_id,member_id)
                        df_member = df[df.member_id==member_id]
                        file = df_member.values[0]
                        zarr_dir = dict(zip(df.keys(),file))
                        zarr_file = zarr_format % zarr_dir
   
                        zstore = 'gs://cmip6' + zarr_file + '/'
                        df_cloud = dfm[dfm.zstore==zstore]
                        if len(df_cloud) >= 1:
                            #print('store already in cloud')
                            continue
                        else:
                            ngood += 1
  
                        with warnings.catch_warnings():
                            warnings.filterwarnings("ignore")
                            df_member.loc[:,'zstore'] = zarr_file

                        df_list += [df_member]

    if ngood >= 1:
        return pd.concat(df_list)
    else: 
        #print('no new data available')
        return dESGF[dESGF.source_id=='junk']
