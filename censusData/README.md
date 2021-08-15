# Census Data
by Lucy Brock

This project retreives population and gdp data at national, state, msa, and county levels, as well as population data for neighborhoods and LA and Orange County subcounties. 

## What this application does:
For the population and gdp data for the four larger geographies, I used API calls to government census and BEA data. For the neighborhoods and subcounty populations, I retreived population data at the census tract level, matched those populations to census tract shapefiles, and then overlayed those shapefiles with neighborhood and subcounty shapefiles, matching the census tract population to the neighborhood it is mostly contained in. I then formatted all of the information and put it in postgres. 

## What technologies/techniques it uses:
API calls, pandas, geopandas

## Challenges and future implementations:
I'm reworking the loops, as they take too long. 

## Installation
I ran this on spyder. It requires all of the packages listed in my code, as well as signing up for census data and BEA api keys. I also linked this to my postgres account, but the information could be easily exported to a csv file.

## License
[GNU General Public License v3.0](https://choosealicense.com/licenses/gpl-3.0/#)
