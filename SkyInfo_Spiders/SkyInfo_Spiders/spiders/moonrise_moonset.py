import scrapy


class MoonriseMoonsetSpider(scrapy.Spider):
    name = 'moonrise_moonset'
    allowed_domains = ['www.timeanddate.com']
    start_urls = ['http://www.timeanddate.com/']

    def parse(self, response):
        pass
