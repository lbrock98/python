# Google Trends
by Lucy Brock

Ranks Fortune 500 companies in terms of how much they were trending on google's news results in the United States in the past year. 

## What this application does:
This ranking was created using Google Trends, a website that analyzes the popularity of search queries in Google Search. Google Trends analyzes a maximum of five queries at a time. The number of searches for each query is divided by the number of total searches within a geographic region and timeframe, and then each query is scaled on a range of 0 to 100 (zero meaning there is insufficent data, one hundred meaning a term is at peak popularity). Five companies are compared at a time, and the company with the most searches is selected. The other four companies are scaled by this highest ranking company. The highest ranking company is then compared against the next four companies, and so on, scaling all previous companies if the highest ranking company changed. 
All companies are now scaled against the highest ranking company, which was given a value of 100,000. 

## What technologies/techniques it uses:
Pandas, loops, csv

## Challenges and future implementations:
The runtime isn't ideal. 

## Installation
I ran this on spyder. 

## License
[GNU General Public License v3.0](https://choosealicense.com/licenses/gpl-3.0/#)
