from SkyInfo_Spiders.SkyInfo_Spiders.spiders import meteo, moon_phases, moonrise_moonset, sunrise_sunset
from LightPrice_Spiders.LightPrice_Spiders.spiders import light_price

from Preprocessor.Preprocessor import Preprocessor

from Light_intensity_recommender.Light_intensity_recommender import LightIntensityRecommender
import argparse 

class Gamification():
        
    def run_scrapies():

        meteo.run_scrapy()
        moon_phases.run_spider()
        moonrise_moonset.run_spider()
        sunrise_sunset.run_spider()

        light_price.run_spider()

        #rss


    def run_preprocessor():
        preprocessor = Preprocessor()
        preprocessor.get_input_data()
        preprocessor.preprocess_data()
        preprocessor.save_output_data()


    def run_light_recommender(params):
        parser = argparse.ArgumentParser()
        parser.add_argument("--params", help="List of the municipalities the user wants to preprocess", nargs='+', default=["light price", "moon", "snow", "rain", "cloud"])
        
        args = parser.parse_args()
        params = args.params

        light_intensity_recommender = LightIntensityRecommender()
        light_intensity_recommender.get_input_data()
        light_intensity_recommender.calculate_recommended_light_intensity(params)
        light_intensity_recommender.calculate_energy_savings()
        light_intensity_recommender.save_output_data()


def main():
    gamification = Gamification()
    gamification.run_scrapies()
    gamification.run_preprocessor()
    gamification.run_light_recommender(params)

if __name__ == "__main__":
    # Setting the command line argument to customize the recommender    
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", help="List of the municipalities the user wants to preprocess", nargs='+', default=["light price", "moon", "snow", "rain", "cloud"])
        
    args = parser.parse_args()
    params = args.params

    # Call to the main function
    main(params)