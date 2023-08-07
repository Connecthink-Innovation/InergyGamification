from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from functools import reduce
import pandas as pd
import time   
from datetime import datetime, timedelta
import os


def render_page(url):
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(3)
    r = driver.page_source
    driver.quit()
    return r

def scraper_next_day(page, dates, project_root):
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
            

    output = pd.DataFrame(data, columns=['Date', 'Hour', 'condition', 'temp_farenheit', 'feels_like_farenheit', 'precip_percent', 'precip_inches', 'cloud_cover_percent', 'dew_point_farenheit', 'humidity_percent', 'wind_vel', 'pressure_inches'])
    file_path = os.path.join(project_root, "SkyInfo_Spiders", "data", "weather_next.csv")
    output.to_csv(file_path)

    print('Scraper done!')


    return output


def scraper_previous_days(page, dates, project_root):
    output = pd.DataFrame()
    
    data = []
    for d in dates:

        url = str(str(page) + str(d))

        r = render_page(url)

        soup = BS(r, "html.parser")
        container = soup.find('lib-city-history-observation')
        check = container.find('tbody')


        for c in check.find_all('tr', class_='ng-star-inserted'):
            data_row = []
            data_row.append(d)

            for i in c.find_all('td', class_='ng-star-inserted'):
                trial = i.text.strip('  ')
                data_row.append(trial)
            
            data.append(data_row)
            
    output = pd.DataFrame(data, columns=["Date", "Hour", "temp_farenheit", "dew_point_farenheit", "humidity_percent", "wind", "wind_vel", "wind_gust", "pressure_inches", "precip_inches", "condition"])

    file_path = os.path.join(project_root, "SkyInfo_Spiders", "data", "weather_previous.csv")
    output.to_csv(file_path)

    print('Scraper done!')


    return output



# Debugger
def run_scrapy(mode, project_root):

    dates = []

   
    #PROD. CODE
    if mode == "prod":
        date = datetime.now()
        for i in range(2, -1, -1):
            date_previous_i = date - timedelta(days=i)
            date_previous_i_str = date_previous_i.strftime("%Y-%m-%d")
            dates.append(date_previous_i_str)

    
    #DEBUG. CODE
    if mode == "debug":
        dates = ["2023-06-26", "2023-06-25", "2023-06-24"]
    #---------------
    
    page = 'https://www.wunderground.com/history/daily/es/canyelles/ICANYE10/date/'
    df_output = scraper_previous_days(page, dates, project_root) 

    dates = []

    
    #PROD. CODE
    if mode == "prod":
        date_actual = datetime.now()
        date_actual_str = date_actual.strftime("%Y-%m-%d")
        date_next = datetime.now() + timedelta(days=1)
        date_next_str = date_next.strftime("%Y-%m-%d")

    
    #DEBUG. CODE
    if mode == "debug":
        date_actual_str = "2023-06-26"
        date_next_str = "2023-06-27"
    #---------------

    dates.append(date_actual_str)
    dates.append(date_next_str)

    #PROD. CODE
    if mode == "prod":
        page = 'https://www.wunderground.com/hourly/es/canyelles/ICANYE10/date/'
        df_output2 = scraper_next_day(page,dates)

    #DEBUG. CODE
    if mode == "debug":
        page = 'https://www.wunderground.com/history/daily/es/canyelles/ICANYE10/date/'
        df_output2 = scraper_previous_days(page, dates, project_root)

#run_scrapy(mode="debug", project_root=r"c:\Users\abelb\Desktop\Gamification" )
