from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from functools import reduce
import pandas as pd
import time   


def render_page(url):
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(3)
    r = driver.page_source
    driver.quit()
    return r

def scraper(page, dates):
    output = pd.DataFrame()
    
    data = []
    for d in dates:

        url = str(str(page) + str(d))

        r = render_page(url)

        soup = BS(r, "html.parser")
        container = soup.find('lib-city-hourly-forecast')
        check = container.find('tbody')


        for c in check.find_all('tr', class_='ng-star-inserted'):
            data_row = []
            data_row.append(d)

            for i in c.find_all('td', class_='ng-star-inserted'):
                trial = i.text.strip('  ')
                data_row.append(trial)
            
            data.append(data_row)
            

    output = pd.DataFrame(data, columns=['day', 'time', 'condition', 'temp', 'feels_like', 'precip_percent', 'precip_amount', 'cloud_cover', 'dew_point', 'humidity_percent', 'wind_vel', 'pressure'])
    output.to_csv("SkyInfo_Spiders/data/weather.csv")

    print('Scraper done!')


    return output



# Debugger
def run_scrapy():
    dates = ['2023-06-14','2023-06-15']
    page = 'https://www.wunderground.com/hourly/es/canyelles/ICANYE10/date/'
    df_output = scraper(page,dates)

run_scrapy()