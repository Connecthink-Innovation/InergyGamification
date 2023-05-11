from scrapy import Spider, signals, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os


class MoonPhasesSpider(Spider):
    name = 'moon_phases'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/moon/phases']

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.search_cities = [("spain", "barcelona")] #!minus
        self.years = ["2022", "2023"]
        self.allowed_domains = ['www.timeanddate.com']
        self.start_urls = ['http://www.timeanddate.com/moon/phases']


    def start_requests(self):

        for url in self.start_urls:
            for city in self.search_cities:
                for year in self.years:
                
                    url_parse = "/".join([url, city[0], city[1]]) #Add city
                    url_parse += "?year=" + year #Add year
                    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
                    print("Request to:", url_parse)
                    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
                    yield Request(url_parse, callback=self.parse)

    def parse(self, response):

        lunations = []

        rows = response.xpath('/html/body/div[5]/main/article/section[2]/div[2]/div/table//tr')
        header = [cell.xpath(".//text()").get().strip() for cell in rows[0].xpath(".//th")]

        for row in rows:
            values = [cell.xpath(".//text()").get().strip() for cell in row[0].xpath(".//td")]

            data = {}
            for i, col in enumerate(header):
                data[col] = values[i]

            lunations.append(data)
        
        self.save_csv(lunations)

    def save_csv(self, response):
        filename = response.url.split("/")[-1]
        path = os.path.join("data", filename)
        with open(path, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')


# Debugger
def run_spider():
    process = CrawlerProcess(get_project_settings())
    process.crawl(MoonPhasesSpider)
    process.start()

run_spider()