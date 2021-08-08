# gets census population data for states, MSAs, counties, and census tracts
import urllib
from psycopg2.extras import RealDictCursor
import psycopg2
import json
import pandas as pd

year = 2019

# get population of US
portfolio = urllib.request.urlopen(f'https://api.census.gov/data/{year}/pep/population?get=NAME,POP&for=us:*&key=fe6d5b03221189e31343c50fb863de67db0d30e9')
p_data = portfolio.read().decode('utf-8')
p_data = p_data[1:-1]
header = p_data.split('],')[0] + ']'
p_data = p_data.replace(header, '')
p_data = '[' + p_data[2:] + ']'
p_pop = pd.DataFrame(eval(p_data), columns = ['name', 'pop', 'geo_id'])
p_pop['year'] = year
p_pop['geo_type'] = 'portfolio'
p_pop['metric'] = 'population'

# get population by state 
state = urllib.request.urlopen(f'https://api.census.gov/data/{year}/pep/population?get=NAME,POP&for=state:*&key=fe6d5b03221189e31343c50fb863de67db0d30e9')
s_data = state.read().decode('utf-8')
s_data = s_data[1:-1]
header = s_data.split('],')[0] + ']'
s_data = s_data.replace(header, '')
s_data = '[' + s_data[2:] + ']'
s_pop = pd.DataFrame(eval(s_data), columns = ['name', 'pop', 'geo_id'])
s_pop['year'] = year
s_pop['geo_type'] = 'state'
s_pop['metric'] = 'population'

# get population by MSA 
msa = urllib.request.urlopen(f'https://api.census.gov/data/{year}/pep/population?get=NAME,POP&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key=fe6d5b03221189e31343c50fb863de67db0d30e9')
m_data = msa.read().decode('utf-8')
m_data = m_data[1:-1]
header = m_data.split('],')[0] + ']'
m_data = m_data.replace(header, '')
m_data = '[' + m_data[2:] + ']'
m_pop = pd.DataFrame(eval(m_data), columns = ['name', 'pop', 'geo_id'])
# reformat to add state fips to geo_id
for i in range(len(m_pop)): 
    if 'PR' in m_pop['name'][i]:
        m_pop = m_pop.drop([i])
m_pop = m_pop.reset_index(drop = True)
state_fips = pd.read_csv("/Users/Lucy/Documents/Work/Ichor/censusData/state_fips.csv")
m_pop['state'] = m_pop['geo_id']
for i in range(len(m_pop)): 
    x = m_pop['name'][i].split(', ')[1][:2]
    for j in range(len(state_fips)):
        if x == state_fips['abbrv'][j]:
            m_pop['state'][i] = '{:02d}'.format(state_fips['num'][j])
            break
m_pop['geo_id'] = m_pop['state'] + m_pop['geo_id'] 
m_pop.drop(columns = ['state'], inplace = True)
m_pop['year'] = year
m_pop['geo_type'] = 'msa'
m_pop['metric'] = 'population'

# get population by county 
county = urllib.request.urlopen(f'https://api.census.gov/data/{year}/pep/population?get=NAME,POP&for=county:*&in=state:*&key=fe6d5b03221189e31343c50fb863de67db0d30e9')
c_data = county.read().decode('utf-8')
c_data = c_data[1:-1]
header = c_data.split('],')[0] + ']'
c_data = c_data.replace(header, '')
c_data = '[' + c_data[2:] + ']'
c_pop = pd.DataFrame(eval(c_data), columns = ['name', 'pop', 'state', 'county'])
c_pop['geo_id'] = c_pop['state'] + c_pop['county'] 
c_pop.drop(columns = ['state', 'county'], inplace = True)
c_pop['year'] = year
c_pop['geo_type'] = 'county'
c_pop['metric'] = 'population'

# reformat final table
populations = [p_pop, s_pop, m_pop, c_pop]
final = pd.concat(populations)
cols = final.columns.tolist()
reorder = [3, 4, 2, 5, 1]
cols = [cols[i] for i in reorder]
final = final[cols]

#export to csv 
final.to_csv('/Users/Lucy/Documents/Work/Ichor/censusData/population/others_pop.csv', header=False, index=False)

#export to pgadmin
with open("/Users/Lucy/Documents/Work/Ichor/censusData/pgAdmin_info.json") as f:
    conf=json.load(f)

conn = psycopg2.connect("dbname="+conf['dbname']+" user="+conf['user']+" password="+conf['password']+" host="+conf['host']+" port="+conf['port'])  
cur = conn.cursor(cursor_factory=RealDictCursor)

file = open('/Users/Lucy/Documents/Work/Ichor/censusData/population/others_pop.csv')

cur.copy_from(file,'population.census_data', sep=',', columns = ('year', 'geo_type', 'geo_id', 'metric', 'metric_value'), null = '') 

conn.commit()