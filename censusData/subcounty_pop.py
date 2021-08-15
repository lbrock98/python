#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 11:40:10 2021

This file uses a file of US populations by census tract, a census tract shapefile,
and a subcounty shapefile to get subcounty populations for LA county and Orange county. 

Note that for census tracts that span multiple subcounties, the population for the 
census tract is matched to only one of these subcounties, so as not to count it 
multiple times. 
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

#get population by census tract
f = urllib.request.urlopen(f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B01001_001E&for=tract:*&in=state:06&key={CENSUS_KEY}')
s = f.read().decode('utf-8')
s = s[1:-1]
header = s.split('],')[0] + ']'
data = s.replace(header, '')
data = '[' + data[2:] + ']'
caPop = pd.DataFrame(eval(data), columns = ['name', 'pop', 'state', 'county', 'tract'])

#get only oc/la census tract populations
la_pop = caPop['county'] == '037'
laPop= caPop[la_pop]
oc_pop = caPop['county'] == '059'
ocPop = caPop[oc_pop]
allPop = laPop.append(ocPop)
allPop = allPop.reset_index(drop = True)

#create geo_id to match to shapefiles later
geo_id = []
for i in range(len(allPop)):
    geo = ('{:0>2}'.format(allPop['state'][i])) + ('{:0>3}'.format(allPop['county'][i])) + ('{:0>6}'.format(allPop['tract'][i]))
    geo_id.append(geo)
allPop['GEOID'] = geo_id

#import california census tract shapefile
urllib.request.urlretrieve(f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_06_tract.zip", 
               f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_06_tract.zip")
with zipfile.ZipFile(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_06_tract.zip",'r') as zip_ref:
    zip_ref.extractall(f'/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_06_tract')
os.remove(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_06_tract.zip")    
tracts = gp.read_file(f"/Users/LBrock/Documents/censusData/shapefiles/tl_{year}_06_tract/tl_{year}_06_tract.shp")
tracts.drop(columns = ['NAME', 'NAMELSAD', 'MTFCC', 'FUNCSTAT', 'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON'], inplace = True)

#add population data to census tracts geodataframe
allPop.drop(columns = ['name', 'state', 'county', 'tract'], inplace = True)
tracts = tracts.merge(allPop, on='GEOID')

#import subcounty shapefile
sub = gp.read_file(f"/Users/LBrock/Documents/censusData/shapefiles/subcounty_shapefile/cb_{year}_us_cousub_500k.shp")
sub.drop(columns = ['COUSUBNS', 'AFFGEOID', 'LSAD', 'ALAND', 'AWATER'], inplace = True)

#filter subcounty to those within LA county and orange county
ca = sub['STATEFP'] == '06'
sub_ca = sub[ca]

la = sub_ca['COUNTYFP'] == '037'
sub_ca_la = sub_ca[la]

oc = sub_ca['COUNTYFP'] == '059'
sub_ca_oc = sub_ca[oc]

ca_subcounties = sub_ca_la.append(sub_ca_oc)

#overlay CA subcounties with census tracts 
merged1 = gp.overlay(ca_subcounties, tracts, how='intersection')
#merged1 = gp.sjoin(sub, tracts, how='left', op='intersects')
merged = merged1
merged = merged.dropna()
merged = merged.drop_duplicates(subset=['GEOID_2'])
merged = merged.reset_index(drop = True)
merged['pop'] = merged['pop'].astype(int)
merged2 = merged

#sum populations for subcounties, insert into table
final = pd.DataFrame(columns = ['name', 'pop', 'geo_id'])
i = 0
while i<(len(merged)):
    pop = merged['pop'][i]
    j = 1
    while j<len(merged):
        if merged['NAME'][i] == merged['NAME'][j]:
            pop += merged['pop'][j]
            merged = merged.drop(j)
            merged = merged.reset_index(drop = True)
        else:
            j+=1
    final.loc[len(final)] = [merged['NAME'][i], pop, merged['GEOID_1'][i]]
    merged = merged.drop(i)
    merged = merged.reset_index(drop = True)
    i = 0

#reformat final datatable
final['year'] = year
final['geo_type'] = 'subcounty'
final['metric'] = 'population'

cols = final.columns.tolist()
reorder = [3, 4, 2, 5, 1]
cols = [cols[i] for i in reorder]
final = final[cols]

#export to csv 
final.to_csv('/Users/LBrock/Documents/censusData/population/CA_subcounties.csv', header=False, index=False)

#export to pgAdmin
with open("/Users/LBrock/Documents/censusData/pgAdmin_info.json") as f:
    conf=json.load(f)

conn = psycopg2.connect("dbname="+conf['dbname']+" user="+conf['user']+" password="+conf['password']+" host="+conf['host']+" port="+conf['port'])  
cur = conn.cursor(cursor_factory=RealDictCursor)

file = open('/Users/LBrock/Documents/censusData/population/CA_subcounties.csv')

cur.copy_from(file,'population.census_data', sep=',', columns = ('year', 'geo_type', 'geo_id', 'metric', 'metric_value'), null = '') 
conn.commit()
