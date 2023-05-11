import scrapy
import csv

class GuiaBarcelonaEventsSpider(scrapy.Spider):
    name = 'guia_barcelona_events'
    allowed_domains = ['opendata-ajuntament.barcelona.cat']
    start_urls = ['https://guia.barcelona.cat/agenda']

    def parse(self, response):

        #Lista donde guardaremos los datos extraidos:
        events=[]
        
        events_xpath = response.xpath('//*[@id="agenda-recomenada"]')
        for event in events_xpath:
            event_xpath = event.xpath('//*[@id="agenda-recomenada"]/div/div') 

            properes = event_xpath.xpath('//*[@id="agenda-recomenada"]/div/div/h3').get()
            resum = event_xpath.xpath('//*[@id="agenda-recomenada"]/div/div/p').get()
            dades = event_xpath.xpath('//*[@id="agenda-recomenada"]/div/div/div').get()

            events.append(
                {
                'properes': properes,
                'dades': dades,
                'resum': resum
                }
            )

            print(properes, resum, dades)
        
        #Guardar datos extraidos en un csv
        self.save_csv(events)
            
        
    def save_csv(self, events):
        with open("data/guia_barcelona_events.csv.csv", "w", newline="") as csvfile:
            fieldnames = ["properes", "resum", "dades"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for event in events:
                writer.writerow(event)



###


###