from scrapy import Spider, signals, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
import csv

class MoonriseMoonsetSpider(Spider):
    name = 'moonrise_moonset'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_cities = [("spain", "barcelona")] # Specify the search cities
        self.years_months = {"2022":["1", "2"]}  # Specify the years  and months to search
        self.allowed_domains = ['www.timeanddate.com']
        self.start_urls = ['http://www.timeanddate.com/moon'] #Specify the start url

    def start_requests(self):
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
        
        rows = table.xpath('.//tbody//tr') #Get the rows of the table
        for row in rows: #Iter. over the rows
            values_cells = row.xpath('.//td//text()').extract() #Extract all cells of the actual row
            values = [cell.strip() for cell in values_cells if '↑' not in cell and '°' not in cell and cell.strip()]  # Clean the cell values
            
            # Remove the bad Moonrise element and get the index
            remove_indices = [i for i, value in enumerate(values) if value == "-"]
            for index in reversed(remove_indices):
                del values[index]

            # Delete the correspondent header
            header_row = [value for i, value in enumerate(self.header) if i not in remove_indices]

            data = dict(zip(header_row, values)) #Create dict with column name and value
            data['Year'] = response.meta['year']  # Add the year to the data
            data['Month'] = response.meta['month'] # Add the month to the data
            data['Day']  = row.xpath('.//th//text()').extract()[0] # Add the day to the data
            lunations.append(data) #Save row-dict in the final data-list

        self.save_csv(lunations) 


    def save_csv(self, lunations):
        file_path = "data/moon_phases.csv"

        existing_rows = []  # List to store existing rows

        # Read existing rows from the CSV file
        if os.path.isfile(file_path):
            with open(file_path, "r", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                existing_rows = list(reader)

        # Open the CSV file in append mode to add new rows
        with open(file_path, "a", newline="") as csvfile:
            fieldnames = self.header + ['Year']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header only if the file is empty
            if not existing_rows:
                writer.writeheader()

            # Iterate over new rows and add them if they don't exist in the file
            for lunation in lunations:
                if lunation not in existing_rows:
                    writer.writerow(lunation)
                    existing_rows.append(lunation)

# Debugger
def run_spider():
    process = CrawlerProcess(get_project_settings())
    process.crawl(MoonriseMoonsetSpider)
    process.start()

run_spider()