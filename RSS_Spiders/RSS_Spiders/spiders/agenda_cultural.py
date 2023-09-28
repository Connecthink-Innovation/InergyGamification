import scrapy
import os

class AgendaCulturalSpider(scrapy.Spider):
    name = "agenda_cultural"
    start_urls = [
        "https://opendata-ajuntament.barcelona.cat/data/es/dataset/agenda-cultural"
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Encontrar el enlace al CSV en la páginascrapy crawl agenda_diaria
        csv_link = response.xpath('//*[@id="historic-series"]/ul/li[1]/div/ul/li[1]/a/@href') 

        # Descargar el CSV
        yield response.follow(csv_link[0], callback=self.save_csv)

    def save_csv(self, response):
        filename = response.url.split("/")[-1]
        path = os.path.join("data", filename)
        with open(path, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')


