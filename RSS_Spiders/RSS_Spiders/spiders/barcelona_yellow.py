import scrapy
import csv

class BarcelonaYellowSpider(scrapy.Spider):
    name = "barcelona_yellow"
    start_urls = [
        "https://www.barcelonayellow.com/barcelona-events-calendar"
    ]

    def parse(self, response):
    
        events = []
        for event in response.xpath('//*[@class="eventtable"]//tbody//tr'): 
            events.append({ 
                "date": event.xpath("td[1]//text()[normalize-space()]").get(), 
                "event": event.xpath("td[2]//text()").get(),
                "location": event.xpath("td[3]/a/text()").get(),
                "category": event.xpath("td[4]/a/text()").get(),
            })
      
        self.save_csv(events)

        


    def save_csv(self, events):
        with open("data/barcelona_yellow_events.csv", "w", newline="") as csvfile:
            fieldnames = ["date", "event", "location", "category"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for event in events:
                writer.writerow(event)


