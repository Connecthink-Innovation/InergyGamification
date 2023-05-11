import scrapy


class MoonPhasesSpider(scrapy.Spider):
    name = 'moon_phases'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']

    def parse(self, response):
        pass
