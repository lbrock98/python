#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 27 14:32:37 2021

@author: Lucy
"""
import urllib
import json
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

year=2019

#read state data
read_state = urllib.request.urlopen(f'https://apps.bea.gov/api/data/?&UserID={BEA_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP1&LineCode=1&GeoFIPS=state&year={year}&ResultFormat=json')
state_file = read_state.read().decode('utf-8')
state_file = state_file.split('"Data":')[1]
state_file = state_file.split(',"Notes"')[0]
state = pd.read_json(state_file)
state.drop(state.tail(8).index, inplace = True)
state['geo_type'] = 'state'
state['geo_type'][0] = 'portfolio'
state['metric'] = 'GDP: ' + state['CL_UNIT']
recast = {'GeoFips': str}
state = state.astype(recast)
for i in range(len(state)):
    x = '{:0>5}'.format(state['GeoFips'][i])
    state['GeoFips'][i] = x[:-3]
state.drop(columns = ['Code', 'GeoName', 'UNIT_MULT', 'CL_UNIT'], inplace = True)
cols = state.columns.tolist()
reorder = [1, 3, 0, 4, 2]
cols = [cols[i] for i in reorder]
state = state[cols]

#read msa data
read_msa = urllib.request.urlopen(f'https://apps.bea.gov/api/data/?&UserID={BEA_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP1&LineCode=1&GeoFIPS=msa&year={2019}&ResultFormat=json')
msa_file = read_msa.read().decode('utf-8')
msa_file = msa_file.split('"Data":')[1]
msa_file = msa_file.split(',"Notes"')[0]
msa = pd.read_json(msa_file)
msa['metric'] = 'GDP: ' + msa['CL_UNIT']
msa['geo_type'] = 'msa'
msa = msa.drop([0]) #first record gives the portfolio data again
msa = msa.reset_index(drop = True)
state_fips = pd.read_csv("/Users/LBrock/Documents/state_fips.csv")
msa = msa.astype(recast)
msa['state'] = msa['GeoFips']
for i in range(len(msa)): 
    x = msa['GeoName'][i].split(', ')[1][:2]
    for j in range(len(state_fips)):
        if x == state_fips['abbrv'][j]:
            msa['state'][i] = '{:02d}'.format(state_fips['num'][j])
            break
msa['GeoFips'] = msa['state'] + msa['GeoFips'] 
msa.drop(columns = ['Code', 'UNIT_MULT', 'NoteRef', 'CL_UNIT', 'state', 'GeoName'], inplace = True)
cols = msa.columns.tolist()
reorder = [1, 4, 0, 3, 2]
cols = [cols[i] for i in reorder]
msa = msa[cols]

#read county data
read_county = urllib.request.urlopen(f'https://apps.bea.gov/api/data/?&UserID={BEA_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP1&LineCode=1&GeoFIPS=COUNTY&year={year}&ResultFormat=json')
county_file = read_county.read().decode('ISO-8859-1')
county_file = county_file.split('"Data":')[1]
county_file = county_file.split(',"Notes"')[0]
county = pd.read_json(county_file)
county['metric'] = 'GDP: ' + county['CL_UNIT']
county['geo_type'] = 'county'
state_fips = pd.read_csv("/Users/LBrock/Documents/state_fips.csv")
county = county.astype(recast)
county['state'] = county['GeoFips']
for i in range(len(county)): 
    x = county['GeoName'][i].split(', ')[1][:2]
    for j in range(len(state_fips)):
        if x == state_fips['abbrv'][j]:
            county['state'][i] = '{:02d}'.format(state_fips['num'][j])
            break
county['GeoFips'] = county['state'] + county['GeoFips'] 
county.drop(columns = ['Code', 'UNIT_MULT', 'NoteRef', 'CL_UNIT', 'state', 'GeoName'], inplace = True)
cols = county.columns.tolist()
reorder = [1, 4, 0, 3, 2]
cols = [cols[i] for i in reorder]
county = county[cols]

# reformat final table
gdp_data = [state, msa, county]
final = pd.concat(gdp_data)
final = final.reset_index(drop = True)
final['DataValue'] = final['DataValue'].map(lambda x: x.replace(',', ''))
final['DataValue'] = final['DataValue'].map(lambda x: x.replace('(NA)', '0'))
recast = {'DataValue': int}
final = final.astype(recast)

#export to csv 
final.to_csv('/Users/LBrock/Documents/censusData/all_gdp.csv', header=False, index=False)

#export to pgadmin
with open("/Users/LBrock/Documents/censusData/pgAdmin_info.json") as f:
    conf=json.load(f)

conn = psycopg2.connect("dbname="+conf['dbname']+" user="+conf['user']+" password="+conf['password']+" host="+conf['host']+" port="+conf['port'])  
cur = conn.cursor(cursor_factory=RealDictCursor)

file = open('/Users/LBrock/Documents/censusData/all_gdp.csv')

cur.copy_from(file,'census_data.data', sep=',', columns = ('year', 'geo_type', 'geo_id', 'metric', 'metric_value'), null = '') 

conn.commit()
