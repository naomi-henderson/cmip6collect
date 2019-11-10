import warnings
import pandas as pd

def identify(dfm, df_req, dESGF, single_member_tables):
    single_member = single_member_tables

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
        table_id = row['table']

        for experiment_id in experiment_ids:
            for variable_id in variable_ids:
                if source_ids == ['All']:
                    sources = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                                   (dESGF.variable_id==variable_id)].source_id.unique()
                else:
                    sources = source_ids
                for source_id in sources:
                    df = dESGF[(dESGF.experiment_id==experiment_id)&(dESGF.table_id==table_id)&
                                (dESGF.variable_id==variable_id)    &(dESGF.source_id==source_id)]
                    member_ids = df.member_id.unique()
                    for member_id in member_ids:
                        #print(experiment_id,variable_id,source_id,member_id)
                        df_member = df[df.member_id==member_id]
                        file=df_member.values[0]
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
