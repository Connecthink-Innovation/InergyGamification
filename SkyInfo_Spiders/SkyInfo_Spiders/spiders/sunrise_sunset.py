from scrapy import Spider, signals, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
import csv

class SunriseSunsetSpider(Spider):
    name = 'sunrise_sunset'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_cities = [("spain", "barcelona")] # Specify the search cities
        self.years_months = {"2022":["1", "2"]}  # Specify the years  and months to search
        self.allowed_domains = ['www.timeanddate.com']
        self.start_urls = ['http://www.timeanddate.com/sun'] #Specify the start url

    def start_requests(self):
            for url in self.start_urls: #Iter. over the urls
                for city in self.search_cities: #Iter. over the cities
                    for year in self.years_months: #Iter. over the years
                        for month in self.years_months[year]:
                            url_parse = "/".join([url, city[0], city[1]]) # Build the URL with city and year
                            url_parse += "?month=" + month + "&year=" + year
                            yield Request(url_parse, callback=self.parse, meta={'year': year, 'month': month})  # Pass the year as metadata. Is important to then create the csv and be able to add the year

    def parse(self, response):
            days = []

            table = response.xpath('//table[@id="as-monthsun"]') #Save xpath of the table to scrape
            header_cells = table.xpath('.//thead//th/text()').getall() # Get headers of the table
            self.header = [cell.strip() for cell in header_cells[8:]]  # Clean the header cells
            for i in [4, 5]:
                self.header[i] = self.header[i] + "_astronomical_twilight"
            for i in [6, 7]:
                self.header[i] = self.header[i] + "_nautical_twilight"
            for i in [8, 9]:
                self.header[i] = self.header[i] + "_civil_twilight" #IMPORTANT! period of time before sunrise and after sunset when there is still enough natural light for most outdoor activities without the need for artificial lighting
            self.header[10] = "solar_noon_" + self.header[10]

            rows = table.xpath('.//tbody//tr') #Get the rows of the table
            for row in rows: #Iter. over the rows
                values_cells = row.xpath('.//td//text()').extract() #Extract all cells of the actual row
                values = [cell.strip() for cell in values_cells if '↑' not in cell and '°' not in cell and cell.strip()]  # Clean the cell values
                

                data = dict(zip(self.header, values)) #Create dict with column name and value
                data['Year'] = response.meta['year']  # Add the year to the data
                data['Month'] = response.meta['month'] # Add the month to the data
                data['Day']  = row.xpath('.//th//text()').extract()[0] # Add the day to the data
                days.append(data) #Save row-dict in the final data-list

            self.save_csv(days) 

    def save_csv(self, days):
        file_path = "C:/Users/CT/Desktop/SkyInfo/SkyInfo_Spiders/data/sunrise_sunset.csv"

        existing_rows = []  # List to store existing rows

        # Read existing rows from the CSV file
        if os.path.isfile(file_path):
            with open(file_path, "r", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                existing_rows = list(reader)

        # Open the CSV file in append mode to add new rows
        with open(file_path, "a", newline="") as csvfile:
            fieldnames = self.header + ['Year', 'Month', 'Day']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header only if the file is empty
            if not existing_rows:
                writer.writeheader()

            # Iterate over new rows and add them if they don't exist in the file
            for day in days:
                if day not in existing_rows:
                    writer.writerow(day)
                    existing_rows.append(day)


# Debugger
def run_spider():
    process = CrawlerProcess(get_project_settings())
    process.crawl(SunriseSunsetSpider)
    process.start()

run_spider()