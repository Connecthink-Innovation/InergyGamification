import logging
import urllib3
from selenium.webdriver.remote.remote_connection import LOGGER as selenium_logger

# Desactivar los mensajes de registro de Selenium, scrapy y urllib3
logging.getLogger('selenium').setLevel(logging.WARNING)
selenium_logger.setLevel(logging.WARNING)
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
urllib3_logger = logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('filelock').setLevel(logging.ERROR)


#scrapy
import scrapy

from scrapy_selenium import SeleniumRequest

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException

#debug
from scrapy import cmdline
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess

#utils
import os
from time import sleep
import random 
import copy
import pandas as pd
import re
from datetime import datetime, timedelta


class GoogleEventsSpider(scrapy.Spider):
    name = "googleevents"
    allowed_domains = ['google.com']
    start_urls = ['http://google.com/']

    def __init__(self, project_root, *args, **kwargs):
        
        self.project_root = project_root

        self.query_keywords = ['events+Barcelona']

        self.locations = ['Barcelona'] #Esto se pasara por argumento

        self.extrapolate_to = [{'Location':'Plaça del casal de Canyelles', 'lat':41.285159447102856 , 'lon':1.7217341064298708}, {'Location':'Jardins el hotelito Canyelles urbanització California', 'lat':41.267869034171675, 'lon':1.724039265956549}, {'Location':'Castell de Canyelles', 'lat':41.28713224852074, 'lon':1.722669984574967}] #Esto se pasara por argumento
                          
  
        self.event_constructor = {
            "Title":None,
            "Schedule":None,
            "Location":None,
            "lat":None,
            "lon":None,
            "Description":None,
        }

        self.df = pd.DataFrame(columns=self.event_constructor.keys())

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
            'Accept-Language': '*',
            'Referer': 'https://www.google.com',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
    def start_requests(self):

        chrome_options = Options()
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--window-size=1280,720")
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0: Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36')
        chrome_options.add_argument('--no_sandbox')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        chrome_driver_path = os.path.join(self.project_root,"RSS_Spiders", "RSS_Spiders", "chromedriver.exe")
        chrome_service = ChromeService(executable_path=chrome_driver_path)

        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        yield SeleniumRequest(
            url=self.start_urls[0],
            wait_time=10,
            screenshot=True,
            dont_filter=True,
            callback=self.search_events,
            headers=self.headers,
            meta={'driver':driver}
        )

    def search_events(self, response):
        driver = response.meta['driver']
        driver.get(response.url)

        if 'consent.google.com' in response.url:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            boton_accept_cookies = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//*[text()="Aceptar todo"]'))
            )

            boton_accept_cookies.click()

            WebDriverWait(driver,10).until(EC.url_contains('google.com/search'))

        for location in self.locations:
            for query_keyword in self.query_keywords:
    
                yield SeleniumRequest(
                    url=self.get_events_url(query_keyword, location),
                    wait_time=10,
                    screenshot=True,
                    dont_filter=True,
                    callback=self.parse_events,
                    meta={'driver':driver},
                    headers=self.headers
                )

    def get_events_url(self, query_keyword, location):
        query = f'https://www.google.com/search?q={query_keyword}&'
        query += f'sxsrf=AB5stBicut4WeC5bqj9kOLbQWvwhSuysRA:1688455271978&ei=Z8ijZOmnO_bZ7_UPqP6zqAo&uact=5&oq=google+eventos&gs_lcp=Cgxnd3Mtd2l6LXNlcnAQAzIHCCMQigUQJzIFCAAQgAQyBggAEBYQHjIGCAAQFhAeMgYIABAWEB4yBggAEBYQHjIGCAAQFhAeMggIABAWEB4QDzIGCAAQFhAeMgYIABAWEB46CggAEEcQ1gQQsAM6CggAEIoFELADEEM6BwgjELADECc6BwgAEIoFEENKBAhBGABQ6GhYjm5gvW9oA3AAeACAAWGIAecCkgEBNJgBAKABAcABAcgBCg&sclient=gws-wiz-serp&ibp=htl;events&rciv=evn&sa=X&ved=2ahUKEwj3idmQwvT_AhUXif0HHUifCfwQ5rwDKAJ6BAg3EA4#'
        query += f'fpstate=tldetail&htichips=date:today'
        query += f'&near={location}'
        return query


    def parse_events(self, response):
        driver = response.meta['driver']
        driver.get(response.url)

        # Aceptar cookies google
        if 'consent.google.com' in response.url:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            button_accept_cookies = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//*[text()="Aceptar todo"]'))
            )

            button_accept_cookies.click()

            WebDriverWait(driver,10).until(EC.url_contains('google.com/search'))

        sleep(3)

        # Clic en el botón "Hoy" o "Today" para filtrar por eventos de hoy
        button_today = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[contains(text(), "Hoy") or contains(text(), "Today")]'))
        )

        sleep(3)

        button_today.click()

        #Parse events data
        last_event = False
        i = 0
        while not last_event:
            
            if i!=0:
                sleep(1)

            #Get newly loaded listings
            listings = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[@class='odIJnf']")))[i:]
            
            if len(listings) == 0:
                last_event = True
                print("\n\n\n\nNO MORE EVENTS\n\n\n\n")
                continue
            
            sleep(random.uniform(1,2))

            for listing in listings:
                event_data = copy.deepcopy(self.event_constructor)

                try:
                    self.scroll_into_view_events(driver, listing)
                    listing.click()
                except:
                    break

                
                #Get event data
                event_data["Title"] = self._get_event_title(driver)
                event_data["Schedule"] = self._get_event_schedule(driver)
                event_data["Location"] = self._get_event_location(driver)["Location"]
                event_data["lat"] = self._get_event_location(driver)["lat"]
                event_data["lon"] = self._get_event_location(driver)["lon"]
                event_data["Description"] = self._get_event_description(driver)
                
                #Append event data to df
                if event_data["Schedule"]: #Only if exists Schedule Info.
                    self.append_row(event_data)

            i += len(listings)

        self.spider_closed()


    def _get_event_title(self, driver):
        event_title = None

        try:
            event_container = driver.find_element(By.XPATH, '//div[@class="oaJYtf uAAqtb"]')
            event_title_element = event_container.find_element(By.XPATH, './/div[@class="dEuIWb"]')
            event_title = event_title_element.text.strip()

        except Exception as e:
            pass

        sleep(random.uniform(0.5, 1))
        return event_title


    def _get_event_schedule(self, driver):
        event_schedule = None

        try:
            event_container = driver.find_element(By.XPATH, '//div[@class="oaJYtf uAAqtb"]')
            event_schedule_element = event_container.find_element(By.XPATH, './/div/div[@class="Gkoz3"]')            
            event_schedule_text = event_schedule_element.text.strip().replace('\u2009', '')

            text_split = event_schedule_text.split()

            # Obtener la fecha actual y la fecha de mañana
            actual_date_day = str(datetime.now().day)
            tomorrow_date_day = str((datetime.now() + timedelta(days=1)).day)

            # Comparar los números con las fechas 
            if actual_date_day in text_split and tomorrow_date_day in text_split:
                event_schedule = self.format_hours(event_schedule_text)
            
            elif actual_date_day in text_split and tomorrow_date_day not in text_split and len(text_split) < 5:
                event_schedule = self.format_hours(event_schedule_text)


        except Exception as e:
            pass

        sleep(random.uniform(0.5, 1))
        return event_schedule


    # Función para extraer el rango de horas en el formato deseado
    def format_hours(self, time_range):
        try:
            # Buscar las dos horas en el formato HH:MM o H:MM
            pattern = r'\d{1,2}:\d{2}'
            times = re.findall(pattern, time_range)

            # Verificar que se encontraron las dos horas
            if len(times) == 2:
                start_time, end_time = times

                # Convertir el formato H:MM a 0H:MM si es necesario
                start_time = self.format_single_digit_hour(start_time)
                end_time = self.format_single_digit_hour(end_time)

                # Formatear las horas en el formato deseado HH:MM-HH:MM
                return f'{start_time}-{end_time}'

        except:
            pass

        return None

    def format_single_digit_hour(self, time):
        # Convertir el formato H:MM a 0H:MM si solo tiene un dígito para la hora
        if len(time.split(':')[0]) == 1:
            return '0' + time
        return time

    
    def _get_event_location(self, driver):
        event_location = None

        try:
            event_container = driver.find_element(By.XPATH, '//div[@class="oaJYtf uAAqtb"]')
            event_location_element = event_container.find_element(By.XPATH, './/div/span[@class="n3VjZe"]')
            event_location = event_location_element.text.strip()
            event_location = {"Location":event_location, "lat":None, "lon":None}

        except Exception as e:
            pass

        sleep(random.uniform(0.5, 1))


        #Extrapolate location to other city (canyelles)
        if self.extrapolate_to:
            event_location = self.extrapolate_location(extrapolate_to=self.extrapolate_to)


        return event_location
    

    def extrapolate_location(self, extrapolate_to):
        random_location = random.choice(extrapolate_to)
        return random_location



    def _get_event_description(self, driver):
        event_descrip = None

        try:
            event_container = driver.find_element(By.XPATH, '//div[@class="oaJYtf uAAqtb"]')
            event_descrip_element = event_container.find_element(By.XPATH, './/div/span[@class="PVlUWc"]')
            event_descrip = event_descrip_element.text.strip()

        except Exception as e:
            print(e)

        sleep(random.uniform(0.5, 1))
        return event_descrip


    def scroll_into_view_events(self, driver, element):
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        sleep(random.uniform(1, 2))

    def append_row(self, event_data):
        try:
            row_json = {k:[v] for k, v in event_data.items()}
            row_df = pd.DataFrame.from_dict(row_json)

            self.df = pd.concat([self.df, row_df])
        
        except Exception as e:
            pass


    def spider_closed(self):
        self.save_locally()

    
    def save_locally(self):
        file_path = os.path.join(self.project_root, "RSS_Spiders", "data", "google_events.csv")

        self.df.to_csv(file_path, index=False)

def run_spider(project_root):

    process = CrawlerProcess(get_project_settings())
    process.crawl(GoogleEventsSpider, project_root)
    process.start()

#run_spider(project_root=r"c:\Users\abelb\Desktop\Gamification - main test")
