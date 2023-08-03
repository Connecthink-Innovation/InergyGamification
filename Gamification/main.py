#skyinfo imports
from SkyInfo_Spiders.SkyInfo_Spiders.spiders import meteo, moon_phases, moonrise_moonset, sunrise_sunset

#lightprice imports
from LightPrice_Spiders.LightPrice_Spiders.spiders import light_price

#events imports
from RSS_Spiders.RSS_Spiders.spiders import google_events
from Event_generator import event_generator

from Preprocessor.Preprocessor import Preprocessor

from Light_intensity_recommender.Light_intensity_recommender import LightIntensityRecommender
import argparse 

class Gamification():
        
    def run_scrapies(scrapy_params):

        #Sky info
        meteo.run_scrapy()
        moon_phases.run_spider()
        moonrise_moonset.run_spider()
        sunrise_sunset.run_spider()

        #Light price
        light_price.run_spider()

        #Events
        if events_source == "google":
            google_events.run_spider()
        else:
            event_generator.main()


    def run_preprocessor(events_source):
        preprocessor = Preprocessor(events_source)
        preprocessor.get_input_data()
        preprocessor.preprocess_data()
        preprocessor.save_output_data()


    def run_light_recommender(recommender_params):

        light_intensity_recommender = LightIntensityRecommender()
        light_intensity_recommender.get_input_data()
        light_intensity_recommender.calculate_recommended_light_intensity(recommender_params)
        light_intensity_recommender.calculate_intensity_savings()
        light_intensity_recommender.save_output_data()


def main(events_source, recommender_params):
    gamification = Gamification()
    gamification.run_scrapies(events_source)
    gamification.run_preprocessor(events_source)
    gamification.run_light_recommender(recommender_params)

if __name__ == "__main__":
    # Setting the command line argument to customize the recommender    
    parser = argparse.ArgumentParser()
    parser.add_argument("--events_source", help="Indicates the way to obtain the events (google or generator)", type=str, default="google")
    parser.add_argument("--recommender_params", help="List of the features to use in recommender", nargs='+', default=["light price", "moon", "snow", "rain", "cloud"])

    args = parser.parse_args()
    events_source = args.events_source
    recommender_params = args.recommender_params

    # Call to the main function
    main(events_source, recommender_params)



    parser = argparse.ArgumentParser()
parser.add_argument("--param_str", help="A string argument", type=str)

args = parser.parse_args()