import scrapy


class SunriseSunsetSpider(scrapy.Spider):
    name = 'sunrise_sunset'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']

    def parse(self, response):
        pass
