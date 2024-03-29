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

from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import os
from datetime import datetime, timedelta
import csv


#import light_price_predictor


class LightPriceSpider(Spider):
    name = 'light_price'
    allowed_domains = ['tarifaluzhora.es']
    start_urls = ['https://tarifaluzhora.es']

    def __init__(self, mode, project_root, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        #Mode debug or prod
        self.mode = mode 

        self.project_root = project_root

        #Save actual date and next day date
        self.dates = []
        
        
        #PROD. CODE
        if self.mode == "prod":
            date = datetime.now()
            date_next = datetime.now() + timedelta(days=1)
            date_str = date.strftime("%Y-%m-%d")
            date_next_str = date_next.strftime("%Y-%m-%d")
       

        #PREPROD. CODE
        if self.mode == "debug":
            date_str = "2023-06-26"
            date_next_str = "2023-06-27"
        #---------------
        
        self.dates.append(date_str)
        self.dates.append(date_next_str)

        #Define data headers
        self.header = ["Date", "hour_range", 'light_price_kwh' ]

        #Allowed domains and start url
        self.allowed_domains = ['tarifaluzhora.es']
        self.start_urls = ['https://tarifaluzhora.es']


    def start_requests(self):

        self.delete_csv()

        for url in self.start_urls:
            for date in self.dates:
                #Get the url of light price on the indicated day
                url_parse = "/".join([url, f"?date={date}"])
                yield Request(url_parse, callback=self.parse, meta={'Date': date})
    
    def parse(self, response):
        
        #Create list to save the extracted data
        light_prices = []

        for row in response.xpath('//div[@class="template-tlh__colors--hours"]'): #Select element that contains the row info
                    hour_range = row.xpath('.//div//span/text()')[0].get() #Select element with hour_range info
                    price = row.xpath('.//div//span/text()')[1].get() #Select element with light price
                    date = response.meta["Date"] #Get date from meta-response

                    data = dict(zip(self.header, [date, hour_range, price])) #Crate a dict with header - key , data extracted - value
                    light_prices.append(data) #Add row dict to data list
        
        if len(light_prices) == 0:
            print("ERROR: Energy prices go public at 20:30. Wait until them and rerun the code.")
            #light_prices = light_price_predictor.main(self.project_root)
            

        self.save_csv(light_prices, response.meta["Date"])


    def delete_csv(self,):
        file_path = os.path.join(self.project_root, "LightPrice_Spiders", "data", "light_prices.csv")
        file_exists = os.path.isfile(file_path)
        if file_exists:
            os.remove(file_path)

        
    def save_csv(self, light_prices, date):
        # Check if data path exists
        path_dir = os.path.join(self.project_root, "LightPrice_Spiders", "data")
        
        # Create the directory if it does not exist
        os.makedirs(path_dir, exist_ok=True)

        file_path = os.path.join(path_dir, "light_prices.csv")

        file_exists = os.path.isfile(file_path) #Check if file exists
        
        # Open the CSV file in append mode to add new rows
        with open(file_path, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.header)

            # Write the header if the file doesn't exist
            if not file_exists:
                writer.writeheader()

            # Write all the rows
            writer.writerows(light_prices)

def run_spider(mode, project_root):
    process = CrawlerProcess(get_project_settings())
    process.crawl(LightPriceSpider, mode, project_root)
    process.start()

#run_spider(mode="prod", project_root=r"c:\Users\abelb\Desktop\Gamification")