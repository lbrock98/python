# Importing packages
from contextlib import nullcontext
import requests
from bs4 import BeautifulSoup, SoupStrainer
from selenium import webdriver
import pandas as pd
import time
import csv
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC4
from selenium.webdriver.common.action_chains import ActionChains
import urllib.parse

# limits
querylimit = 30
rate = 1
rate2 = 5

# create instance of google chrome and open website
driver = webdriver.Chrome('/Users/Lucy/Documents/chromedriver')

# get csv info
companies = []
with open('Fortune 50 Comps.csv', 'r') as rf:
    reader = csv.reader(rf, delimiter=',', quotechar='"')
    for row in reader:
        companies.append(row[1])

# create table
budgets = pd.DataFrame(
    columns=['Company', '2017-2018', '2019', '2019-2020', '2020', 'type'])

try:
    # loop to iterate through companies
    for comp in companies:
        lob17_18 = nullcontext
        lob19 = nullcontext
        lob19_20 = nullcontext
        lob20 = nullcontext
        type = nullcontext
        # search for a company
        safeString = urllib.parse.quote_plus(comp)
        driver.get(
            f"https://www.opensecrets.org/search?q={safeString}&type=site")
        time.sleep(float(5))

        # get all elements on the search page
        def getElements():
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            soup = soup.find_all("a", class_="gs-title")
            links = []
            for link in soup:
                links.append(link.get('href'))
            return links

        elements = getElements()
        not_none_values = filter(None.__ne__, elements)
        elements = list(not_none_values)

        # check for the right link
        summary = "summary"
        org = "orgs"
        pac = "pac"
        fl = "federal-lobbying"
        sumLinks = [i for i in elements if summary in i]
        links = []
        if sumLinks != []:
            links = [i for i in sumLinks if org in i]
            type = "orgs"
        if links == []:
            links = [i for i in sumLinks if pac in i]
            type = "pac"
        if links == []:
            links = [i for i in sumLinks if fl in i]
            type = "fl"
        if sumLinks == []:
            totals = [i for i in elements if "totals" in i]
            if totals != []:
                links = [i for i in sumLinks if org in i]
            if links != []:
                type = "link"
            else:
                type = "error"
                lob17_18 = "info unavailable"
                lob19_20 = "info unavailable"
                lob19 = "info unavailable"
                lob20 = "info unavailable"

        # get elements
        try:
            link = links[0]
            driver.get(link)
            time.sleep(float(5))
            if type == "link":
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div/div[1]/div/div/div[2]/ul/li[1]/a').click()
                time.sleep(float(rate2))
                type == "orgs"
            if type == "orgs":
                lob17_18 = "---"
                lob19_20 = "---"
                try:
                    lob19_element = driver.find_elements_by_xpath(
                        '//*[@id="flex-wrapper"]/div[2]/div/div[3]/span[1]')[0]
                    lob19 = lob19_element.text

                    lob20_element = driver.find_elements_by_xpath(
                        '//*[@id="flex-wrapper"]/div[2]/div/div[2]/span[1]')[0]
                    lob20 = lob20_element.text
                except:
                    lob19 = "info unavailable"
                    lob20 = "info unavailable"
            elif type == "pac":
                lob19 = "---"
                lob20 = "---"
                # get 2018 info
                try:
                    editedUrl = link[:-4] + "2018"
                    driver.get(editedUrl)
                    time.sleep(float(rate))
                    lob17_18_element = driver.find_elements_by_xpath(
                        '//*[@id="main"]/div[2]/div/div/div[1]/div[1]/div/div[4]/div/div[2]/div/div[2]/table/tbody/tr[1]/td[2]')[0]
                    lob17_18 = lob17_18_element.text
                except:
                    lob17_18 = "info unavailable"
                # get 2020 info
                try:
                    editedUrl = link[:-4] + "2020"
                    driver.get(editedUrl)
                    time.sleep(float(rate))
                    lob19_20_element = driver.find_elements_by_xpath(
                        '//*[@id="main"]/div[2]/div/div/div[1]/div[1]/div/div[4]/div/div[2]/div/div[2]/table/tbody/tr[1]/td[2]')[0]
                    lob19_20 = lob19_20_element.text
                except:
                    lob19_20 = "info unavailable"
            elif type == "fl":
                lob17_18 = "---"
                lob19_20 = "---"
                try:
                    year = driver.find_elements_by_xpath(
                        '//*[@id="main"]/div[2]/div/div/div[2]/form/div/div[1]')[0]
                    # get 2019 info
                    try:
                        link = link.replace(year.text, "2019")
                    except:
                        link = link.replace("summary?", "summary?cycle=2020&")
                    driver.get(link)
                    time.sleep(float(rate))
                    year19 = driver.find_elements_by_xpath(
                        '//*[@id="main"]/div[2]/div/div/div[2]/form/div/div[1]')[0]
                    if year19.text == "2019":
                        try:
                            lob19_element = driver.find_elements_by_xpath(
                                '//*[@id="main"]/div[2]/div/div/div[1]/div/div[1]/div/div/h3[1]')[0]
                        except:
                            lob19_element = driver.find_elements_by_xpath(
                                '//*[@id="main"]/div[2]/div/div/div[1]/div/div[1]/div/div[1]/h3[1]')[0]
                        finally:
                            lob19 = lob19_element.text
                    else:
                        lob19 = "info unavailable"

                    # get 2020 info
                    link = link.replace("2019", "2020")
                    driver.get(link)
                    time.sleep(float(rate))
                    year20 = driver.find_elements_by_xpath(
                        '//*[@id="main"]/div[2]/div/div/div[2]/form/div/div[1]')[0]
                    if year20.text == "2020":
                        try:
                            lob20_element = driver.find_elements_by_xpath(
                                '//*[@id="main"]/div[2]/div/div/div[1]/div/div[1]/div/div/h3[1]')[0]
                        except:
                            lob20_element = driver.find_elements_by_xpath(
                                '//*[@id="main"]/div[2]/div/div/div[1]/div/div[1]/div/div[1]/h3[1]')[0]
                        finally:
                            lob20 = lob20_element.text
                    else:
                        lob20 = "info unavailable"
                except:
                    lob19 = "info unavailable"
                    lob20 = "info unavailable"
        except TypeError:
            print("Couldn't load ", comp)
        except IndexError:
            print("No info found")
        finally:
            # fill in table
            budgets.loc[len(budgets)] = [comp, lob17_18,
                                         lob19, lob19_20, lob20, type]
            print([comp, lob17_18, lob19, lob19_20, lob20, type])
except:
    print("An Error Occurred")
finally:
    # print table
    budgets.to_excel("Lobbying Budgets1.xlsx")

    # close google chrome
    driver.close()
