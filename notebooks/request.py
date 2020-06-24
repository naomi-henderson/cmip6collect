import requests as req
import pandas as pd
from datetime import datetime
from ast import literal_eval
import os
import xarray as xr
import numpy as np
import os

def getsheet(): 
    KEY = '1SGTSK_h4xWX3gdgpeWeCpL_vhzf6tnGPmxetO1gOlQc'
    SHEET_ID = '1506911698'
    url = f'https://docs.google.com/spreadsheets/d/{KEY}/export?format=csv&id={KEY}&gid={SHEET_ID}'

    temp_file_name = 'test_csv.csv'
    download = req.get(url)

    with open(temp_file_name, 'w', newline='\n') as temp_file:
        temp_file.writelines(download.text.replace('\r\n','\n'))

    df = pd.read_csv(temp_file_name, dtype='unicode',keep_default_na=False)
    
    df['members'] = [s.replace(' ','').split(',') for s in df.member_ids.values]
    df['experiments'] = [s.replace('*','').replace(' ','').split(',') for s in df.experiment_ids.values]
    df['models'] = [s.replace('All Available','All').replace(' ','').split(',') for s in df.source_ids.values]
    df['variables'] = [s.replace(' ','').split(',') for s in df['variable_ids (comma separated list)'].values]
    df['table'] = [s.replace(' ','').split(':')[0] for s in df.table_id.values]
    df['requester'] = df['Your name'] 
    df['science'] = df['Science Question/Motivation'] 
    df['comments'] = df['Questions and comments'] 

    df = df.drop(['Your name', 'Science Question/Motivation',
                  'Have you verified the existence of the data you will request?',
                  'table_id', 'source_ids', 'experiment_ids','member_ids',
                  'variable_ids (comma separated list)', 'Questions and comments'],1) 
    return df

def requests(df_prior,rows=[],emails=[],tables=[]): 

    df = df_prior.copy()
    
    if len(rows)+len(emails)+len(tables) == 0:
        df = df[df['requester']=='nutter']
    
    if len(rows) > 0:
        df = df.iloc[rows]  
        
    if len(emails) > 0:
        dk = []
        for email in emails:
            dk += [df[df['E-mail']==email]]     
        df = pd.concat(dk)    
        
    if len(tables) > 0:
        dk = []
        for table in tables:
            print('table',table)
            dk += [df[df['table']==table]]
        df = pd.concat(dk)
        
    df_req = df[df['requester']!='Test'] 
    df_req = df[df['response status']!='once'] 
    
    os.system("/bin/rm -f csv/request_new.csv")
    
    df_all = getsheet()
    
    # save and read back in order to look like df_prior
    df_all.to_csv('csv/request_new.csv',index=False)
    df_all = pd.read_csv('csv/request_new.csv')
      
    df_new = df_all.merge(df_prior, how='left', indicator=True)
    df_new = df_new[df_new['_merge']=='left_only'].drop('_merge',1)

    df_new = pd.concat([df_req,df_new],sort=False)
    
    # convert strings back to lists
    for key in ['experiments','models','variables','members']:
        df_new.loc[:,key] = df_new.loc[:,key].apply(literal_eval)
    
    dtrouble = request_clean(df_new)
    
    return df_new, dtrouble

def set_request_id():
    return datetime.now().strftime('%Y%m%d-%H%M')

def request_clean(df):
    experiments = list(pd.read_csv('csv/Experiments_tier1.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier2.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier3.csv').experiment_id.unique())
    experiments += list(pd.read_csv('csv/Experiments_tier4.csv').experiment_id.unique())
    
    df_var = pd.read_csv('csv/Variables.csv')
    variables = list(df_var.variable_id.unique())

    df_source = pd.read_csv('csv/Models.csv')
    sources = list(df_source.source_id.unique())
    
    dtrouble = {}
    for item,row in df.iterrows():
        troubles = []
        email = row['E-mail']
        if type(email)==type(0.0):
            continue
        table_id = row['table']  
        experiment_ids = row['experiments']
        for experiment_id in experiment_ids:
            if not ((experiment_id in experiments)or(experiment_id=='All')):
                troubles += [f"Warning: {experiment_id} is not a valid experiment_id"]
                
        source_ids = row['models']

        for source_id in source_ids:
            if not ((source_id in sources)or(source_id=='All')):
                troubles += [f"Warning: {source_id} is not a valid source_id"]
                
        variable_ids = row['variables']
        for variable_id in variable_ids:
            if variable_id in variables:
                tables = list(df_var[df_var.variable_id == variable_id].table_id.unique())
                if not (table_id in tables):
                    troubles += [f"Warning: variable_id={variable_id} is not available in table_id={table_id}"]
        
        if len(troubles)>=1:
            print(email,item,troubles)
            dtrouble[email+'_'+str(item)] = troubles

    return dtrouble
