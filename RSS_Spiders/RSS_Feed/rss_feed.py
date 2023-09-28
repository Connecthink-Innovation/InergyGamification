import feedparser
import requests
from bs4 import BeautifulSoup
import csv 
from datetime import datetime
import os
import re

class RssFeedExctractor:
    def __init__(self, rss_url):
        self.rss_url = rss_url
    
        self.PROJECT_PATH = os.getcwd()


    def create_rss(self,):
        rss = feedparser.parse(self.rss_url)

        self.rss_dict = dict(rss)

        #Get list of events
        self.rss_feed = self.rss_dict['feed']
        self.rss_summary = self.rss_feed["summary"]


    def extract_rss_events(self,):

        #Create Soup
        soup = BeautifulSoup(self.rss_summary, 'html.parser')

        event_data = []

        # Search the elements of the list of events page
        event_list = soup.find('div', class_='jcalpro_calendar').find_all('a', class_='jcalpro_calendar_link')

        # Get actual month and year
        current_month = datetime.now().strftime("%m")
        current_year = datetime.now().strftime("%y")

        # Extract the title and date of the events and add in event_data lsit
        for event in event_list:
            title = BeautifulSoup(event['title'], 'html.parser').get_text().split('::')[1] #Use bs4 to remove html tags
            day = event.get_text()
            date = f"{day}/{current_month}/{current_year}"

            #See if +1 events on that day
            events = re.split(r'(?<=[a-záéíóú])(?=[A-ZÀÈÌÒÙÁÉÍÓÚ])', title)
            for event in events:
                event_title = event.strip()     
                event_data.append([event_title, date])


        return event_data


    def save_rss_events(self, event_data):

        file_path = os.path.join(self.PROJECT_PATH, "RSS_Spiders", "data", "rss_canyelles.csv")

        file_exists = os.path.isfile(file_path) #Check if file exists

        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Date']) 
            writer.writerows(event_data)




def run_rss_feed_extractor():
    rss_url = "https://www.canyelles.cat/feed/?post_type=event"

    rss_feed_extractor = RssFeedExctractor(rss_url = rss_url)
    rss_feed_extractor.create_rss()
    event_data = rss_feed_extractor.extract_rss_events()
    rss_feed_extractor.save_rss_events(event_data)

run_rss_feed_extractor()