import csv
import time
import requests 
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError


# play around with limits
querylimit = 30
rate = .1

# get csv info
companies = []
with open('Fortune 500 Companies.csv', 'r') as rf:
    reader = csv.reader(rf, delimiter=',', quotechar='"')
    for row in reader:
        companies.append(row[1])

# create main dataframe and populate it
dfMain = pd.DataFrame(columns = ["Company", "Trending"])
lastMaxComp = companies[0]
i = 1

while i < (len(companies)):
    try:
       # get info from google trends, using pytrends
       pytrend = TrendReq()
       pytrend.build_payload(kw_list=[companies[i], companies[i+1],
                          companies[i+2], companies[i+3], lastMaxComp], gprop='news', timeframe='today 12-m', geo='US')
    except ResponseError:
       print("You've been blacklisted :( ")
       print(f"index: {i}")
    
    except IndexError:
        print("Index Error")
        try: 
            pytrend.build_payload(kw_list=[companies[i], companies[i+1], companies[i+2], lastMaxComp], gprop='news', timeframe='today 12-m', geo='US')
        except IndexError:
            print("Index Error")
            try: 
                pytrend.build_payload(kw_list=[companies[i], companies[i+1],lastMaxComp], gprop='news', timeframe='today 12-m', geo='US')
            except IndexError:
                print("Index Error")
                pytrend.build_payload(kw_list=[companies[i],lastMaxComp], gprop='news', timeframe='today 12-m', geo='US')
                
    # create a dataframe of the mean over time
    df = pytrend.interest_over_time()
    myList = df.mean()

    # get value of last maxComp
    lastMaxVal = myList[len(myList)-1]

    # find the new max value and comp name, move it to the end of list
    # scale each element by the max (the max will be divided by itself, making it 1)
    valList = []
    j = 0
    while j < (len(myList)):
        valList.append(myList[j])
        j+= 1
    maxVal = max(valList)
    if lastMaxVal == maxVal:
        maxComp = lastMaxComp
    else:
        index = valList.index(maxVal)
        maxComp = companies[i+index]
    
    valList.append(valList.pop(valList.index(maxVal)))
    compList = list(df)
    compList.pop()
    compList.append(compList.pop(compList.index(maxComp)))
    scaledList = [x/maxVal for x in valList]

    # if max is new then we have to scale the rest of the elements by the scaled lastMax value
    if dfMain.empty:
        newMax = {'Company': maxComp, 'Trending': 1.0}
        dfMain = dfMain.append(newMax, ignore_index=True)
    elif lastMaxVal != maxVal:
        dfMain['Trending'] = [x * scaledList[3] for x in dfMain['Trending']]
        newMax = {'Company': maxComp, 'Trending': 1.0}
        dfMain = dfMain.append(newMax, ignore_index=True)
        compList.pop()
        scaledList.pop()
    
    # when the dataframe is not empty and the lastMax and max are the same
    # then we don't want to re-add the max to our list, so we only add it
    # given the conditions above, and then pop the max off our lists
    compList.pop()
    scaledList.pop()
    
    # append new values to main dataframe
    dataSubset = {'Company': compList, 'Trending': scaledList}
    dfSubset = pd.DataFrame(dataSubset)
    dfMain = dfMain.append(dfSubset, ignore_index=True)
    
    # update lastMaxComp
    lastMaxComp = maxComp
    # update i
    i += 4


    
# scale values to 100
dfMain['Trending'] = [x * 100000 for x in dfMain['Trending']]
# sort values
dfMain = dfMain.sort_values(by='Trending', ascending=False, ignore_index=True)
print(dfMain)
# print table
dfMain.to_excel("GoogleTrends.xlsx")
