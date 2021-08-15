#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  3 11:07:00 2021


Note that for cities where multiple neighborhoods fall within one census tract,
the census tract's population will be assigned to the neighborhood it falls into most. 
"""
import pandas as pd
import geopandas as gp
import urllib
import zipfile
import os
from psycopg2.extras import RealDictCursor
import psycopg2
import json

year = 2019

#get data by state
final = pd.DataFrame(columns = ['name', 'pop', 'state_id', 'hood_id'])
state = 1
while state < 57:
    #get census tract populations by state
    f = urllib.request.urlopen(f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B01001_001E&for=tract:*&in=state:{state:02d}&key={CENSUS_KEY}')
    s = f.read().decode('utf-8')
    s = s[1:-1]
    header = s.split('],')[0] + ']'
    data = s.replace(header, '')
    data = '[' + data[2:] + ']'
    df_pop = pd.DataFrame(eval(data), columns = ['name', 'pop', 'state', 'county', 'tract'])
    print(f"Got census tract population {state:02d}")
    
    #create geo_id to match to shapefiles later
    geo_id = []
    for i in range(len(df_pop)):
        geo = ('{:0>2}'.format(df_pop['state'][i])) + ('{:0>3}'.format(df_pop['county'][i])) + ('{:0>6}'.format(df_pop['tract'][i]))
        geo_id.append(geo)
    df_pop['GEOID'] = geo_id

    #get census tract shapefile by state
    try:
        urllib.request.urlretrieve(f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{state:02d}_tract.zip", 
                       f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_{state:02d}_tract.zip")
        print(f"Got census tract shapefile {state:02d}")
    except urllib.error.HTTPError:
        print(state)
        state += 1
        continue
    else:
        with zipfile.ZipFile(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_{state:02d}_tract.zip",'r') as zip_ref:
            zip_ref.extractall(f'/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_{state:02d}_tract')
        os.remove(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_{state:02d}_tract.zip")    
        tracts = gp.read_file(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_{state:02d}_tract/tl_{year}_{state:02d}_tract.shp")
        tracts.drop(columns = ['NAME', 'NAMELSAD', 'MTFCC', 'FUNCSTAT', 'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON'], inplace = True)
        print(f"Downloaded shapefile and loaded to geodataframe {state:02d}")

        #add population data to census tracts geodataframe
        tracts = tracts.merge(df_pop, on='GEOID')
    
        #import neighborhood shapefile 
        hoods = gp.read_file('/Users/LBrock/Documents/censusData/hoods.geojson')
        
        #overlay neighborhoods with census tracts 
        merged1 = gp.overlay(hoods, tracts, how='intersection')
        merged = merged1
        merged = merged.dropna()
        merged = merged.reset_index(drop = True)
        merged['pop'] = merged['pop'].astype(int)
    
        print(f"Overlayed neighborhoods and starting loop {state:02d}")
        #sum populations for neighborhoods, insert into table
        i = 0
        while i<(len(merged)): 
            pop = merged['pop'][i]
            j = 1
            while j<len(merged):
                if merged['neighborhood_name'][i] == merged['neighborhood_name'][j]:
                    pop += merged['pop'][j]
                    merged = merged.drop(j)
                    merged = merged.reset_index(drop = True)
                else:
                    j+=1
            final.loc[len(final)] = [merged['neighborhood_name'][i], pop, merged['state_1'][i], merged['hood_id'][i]]
            merged = merged.drop(i)
            merged = merged.reset_index(drop = True)
            i = 0
            
        #update state
        state += 1

#badPop = allPop[~allPop['GEOID'].isin(allTracts['GEOID'])

#reformat final datatable
final['geo_id'] = final['state_id'] + final['hood_id']
final['year'] = year
final['geo_type'] = 'neighborhood'
final['metric'] = 'population'

cols = final.columns.tolist()
reorder = [5, 6, 4, 7, 1]
cols = [cols[i] for i in reorder]
final = final[cols]

#export to csv
final.to_csv('/Users/LBrock/Documents/censusData/population/all_neighborhoods.csv', header=False, index=False)

#export to pgAdmin
with open("/Users/LBrock/Documents/censusData/pgAdmin_info.json") as f:
    conf=json.load(f)

conn = psycopg2.connect("dbname="+conf['dbname']+" user="+conf['user']+" password="+conf['password']+" host="+conf['host']+" port="+conf['port'])  
cur = conn.cursor(cursor_factory=RealDictCursor)

file = open('/Users/LBrock/Documents/censusData/population/all_neighborhoods.csv')

cur.copy_from(file,'population.census_data', sep=',', columns = ('year', 'geo_type', 'geo_id', 'metric', 'metric_value'), null = '') 
conn.commit()
