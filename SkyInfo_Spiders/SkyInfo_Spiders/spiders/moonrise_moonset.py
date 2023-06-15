from scrapy import Spider, signals, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
import csv
from datetime import datetime


class MoonriseMoonsetSpider(Spider):
    name = 'moonrise_moonset'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.PROJECT_PATH = os.getcwd()

        self.search_cities = [("spain", "barcelona")] # Specify the search cities
        """
        #PROD. CODE
        date = datetime.now()
        year = str(date.year)
        month = str(date.month)
        """

        #PREPROD. CODE
        year = "2023"
        month = "6"
        #---------------
        self.years_months = {year:[month]}  # Specify the years  and months to search

        self.allowed_domains = ['www.timeanddate.com']
        self.start_urls = ['http://www.timeanddate.com/moon'] #Specify the start url

    def start_requests(self):

        self.delete_csv()

        for url in self.start_urls: #Iter. over the urls
            for city in self.search_cities: #Iter. over the cities
                for year in self.years_months: #Iter. over the years
                    for month in self.years_months[year]:
                        url_parse = "/".join([url, city[0], city[1]]) # Build the URL with city and year
                        url_parse += "?month=" + month + "&year=" + year
                        yield Request(url_parse, callback=self.parse, meta={'year': year, 'month': month})  # Pass the year as metadata. Is important to then create the csv and be able to add the year

    
    def parse(self, response):
        lunations = []

        table = response.xpath('//table[@id="tb-7dmn"]') #Save xpath of the table to scrape
        header_cells = table.xpath('.//thead//th/text()').getall() # Get headers of the table
        self.header = [cell.strip() for cell in header_cells[4:]]  # Clean the header cells
        self.header[0] = "Moonrise_left"; self.header[2] = "Moonrise_right"
        
        rows = table.xpath('.//tbody//tr') #Get the rows of the table
        for row in rows: #Iter. over the rows
            values_cells = row.xpath('.//td//text()').extract() #Extract all cells of the actual row
            values = [cell.strip() for cell in values_cells if '↑' not in cell and '°' not in cell and cell.strip()]  # Clean the cell values
            
            data = dict(zip(self.header, values)) #Create dict with column name and value
            data['Year'] = response.meta['year']  # Add the year to the data
            data['Month'] = response.meta['month'] # Add the month to the data
            data['Day']  = row.xpath('.//th//text()').extract()[0] # Add the day to the data
            lunations.append(data) #Save row-dict in the final data-list

        self.save_csv(lunations) 


    def delete_csv(self,):
        file_path = os.path.join(self.PROJECT_PATH, "SkyInfo_Spiders", "data", "moonrise_moonset.csv")
        file_exists = os.path.isfile(file_path)
        if file_exists:
            os.remove(file_path)


    def save_csv(self, lunations):
        file_path = os.path.join(self.PROJECT_PATH, "SkyInfo_Spiders", "data", "moonrise_moonset.csv")

        file_exists = os.path.isfile(file_path) #Check if file exists

        # Open the CSV file in append mode to add new rows
        with open(file_path, "a", newline="", encoding="utf-8") as csvfile:
            fieldnames = self.header + ['Year', 'Month', 'Day']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header if the file doesn't exist
            if not file_exists:
                writer.writeheader()

            # Write all the rows
            writer.writerows(lunations)


# Debugger
def run_spider():
    process = CrawlerProcess(get_project_settings())
    process.crawl(MoonriseMoonsetSpider)
    process.start()

run_spider()