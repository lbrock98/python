# Lobbying Budgets
by Lucy Brock

Finds the lobbying budgets of fortune 500 companies from the last few years. 

## What this application does:
Searches for each fortune 500 company on opensecrets.org. Uses a webdriver to search different terms, BeautifulSoup to parse through the urls from the search, and elements' xpath and retreive the actual budgets.

## What technologies/techniques it uses:
BeautifulSoup, selenium, webdriver, pandas

## Challenges and future implementations:
This program is very slow, and I'm working on debugging it. It works very well on fortune 500 companies (whose lobbying budgets are usually available), but has problems with lesser-known companies where the information is unavailable. 

## Installation
I ran this on spyder. 

## License
[GNU General Public License v3.0](https://choosealicense.com/licenses/gpl-3.0/#)
