import numpy as np
import pandas as pd
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            total_size += os.path.getsize(fp)

    return total_size

def get_details(ds,zbdir,zarr):
    # get size, start, stop, length, chunk
    start = 'NA'; stop = 'NA'; nt = 'NA'
    if 'time' in ds.coords:
        dstime = ds.time.values
        start = str(dstime[0])[:10]
        stop = str(dstime[-1])[:10]
        nt = len(dstime)

    size = get_size(zbdir)/1e9
    sizeG = '{:g}'.format(float('{:.3g}'.format(size, p=3)))

    vlist = zarr.split('/')[1:]
    gsurl = 'gs://cmip6' + zarr
    vlist += [gsurl, start, stop, nt, sizeG]
    return vlist

def dict_to_dfcat(zdict):
    dz = pd.DataFrame.from_dict(zdict, orient='index')
    dz = dz.rename(columns={0: "activity_id", 1: "institution_id", 2:"source_id",
                            3:"experiment_id",4:"member_id",5:"table_id",6:"variable_id",
                            7:"grid_label",8:"zstore",9:'date_start',10:'date_stop',11:'time_len',12:'sizeG'})

    dz["dcpp_init_year"] = dz.member_id.map(lambda x: float(x.split("-")[0][1:] if x.startswith("s") else np.nan))
    dz["member_id"] = dz["member_id"].map(lambda x: x.split("-")[-1] if x.startswith("s") else x)
    return dz

def response(df_req,dfcat):
    mails = df_req['E-mail'].unique()

    for mail in mails:
        dfn = df_req.loc[df_req['E-mail'] == mail]

        table_type = 'simple'
        #table_type = 'medium'
        #table_type = 'more'

        for index, row in dfn.iterrows():
            name = row['requester']
            email = row['E-mail']
            experiment_ids = row['experiments']
            source_ids = row['models']
            variable_ids = row['variables']
            member_ids = row['members']
            table_id = row['table']

            if '3hr' in table_id:
                dc = dfcat[(dfcat.table_id == '3hr')|(dfcat.table_id == 'CF3hr')|(dfcat.table_id == 'E3hr')]
                table_id = '3hr'
            else:
                dc = dfcat[dfcat.table_id == table_id]
            print(len(dc),table_id)    
            dcat = []
            if experiment_ids != ['All']:
                for experiment_id in experiment_ids:
                    dcat += [dc[dc.experiment_id == experiment_id]]
                dc = pd.concat(dcat,sort=False)  
            print(len(dc),experiment_ids)    
            dcat = []
            for variable_id in variable_ids:
                dcat += [dc[dc.variable_id == variable_id]]
            dc = pd.concat(dcat,sort=False)
            print(len(dc),variable_ids)  
            dcat = []
            if source_ids != ['All']:
                for source_id in source_ids:
                    dcat += [dc[dc.source_id == source_id]]
                dc = pd.concat(dcat,sort=False)
            if table_type == 'simple':
                dm = dc[['experiment_id','source_id','variable_id','member_id']].groupby([
                         'experiment_id','source_id','variable_id']).nunique()[['member_id']]

                table = pd.DataFrame.pivot_table(dm,
                                                 values='member_id',
                                                 index=['experiment_id','source_id'],
                                                 columns=['variable_id'],
                                                 aggfunc=np.sum,
                                                 fill_value=0)
            elif table_type == 'medium':
                dm = dc[['experiment_id','source_id','variable_id','member_id','grid_label','zstore']].groupby([
                         'experiment_id','source_id','variable_id','grid_label']).nunique()[['member_id']]

                table = pd.DataFrame.pivot_table(dm,
                                                 values='member_id',
                                                 index=['experiment_id','source_id','grid_label'],
                                                 columns=['variable_id'],
                                                 aggfunc=np.sum,
                                                 fill_value=0)


            else:
                dm = dc[['experiment_id','source_id','variable_id','member_id','grid_label','zstore']].groupby([
                         'experiment_id','source_id','variable_id','member_id','grid_label']).nunique()[['zstore']]

                table = pd.DataFrame.pivot_table(dm, values='zstore',
                                                 index=['experiment_id','source_id','member_id','grid_label'],
                                                 columns=['variable_id'],
                                                 aggfunc=np.sum,
                                                 fill_value=0)
    return table
