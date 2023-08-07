#skyinfo imports
from SkyInfo_Spiders.SkyInfo_Spiders.spiders import meteo, moon_phases, moonrise_moonset, sunrise_sunset

#lightprice imports
from LightPrice_Spiders.LightPrice_Spiders.spiders import light_price

#events imports
from RSS_Spiders.RSS_Spiders.spiders import google_events
from Event_generator import event_generator

#preprocessor imports
from Preprocessor.Preprocessor import Preprocessor

#recommender imports
from Light_intensity_recommender.Light_intensity_recommender import LightIntensityRecommender

#visualizer imports
from Visualizer.Visualizer import Visualizer

#utils
import argparse 

class Gamification():
        
    def run_scrapies(events_source):

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


    def run_light_intensity_recommender(recommender_params):

        light_intensity_recommender = LightIntensityRecommender()
        light_intensity_recommender.get_input_data()
        light_intensity_recommender.calculate_recommended_light_intensity(recommender_params)
        light_intensity_recommender.calculate_intensity_savings()
        light_intensity_recommender.calculate_co2_consumption()
        light_intensity_recommender.save_output_data()

    def run_visualizer():
        visualizer = Visualizer()
        visualizer.get_input_data()
        visualizer.visualize_real_intensity_vs_recommended_individual(type="bar")
        visualizer.visualize_zone_intensity_savings()
        visualizer.save_output_data()

def main(events_source, recommender_params):
    gamification = Gamification()
    gamification.run_scrapies(events_source)
    gamification.run_preprocessor(events_source)
    gamification.run_light_intensity_recommender(recommender_params)
    
    if plot_results:
        gamification.run_visualizer()

if __name__ == "__main__":
    # Setting the command line argument to customize the recommender    
    parser = argparse.ArgumentParser()
    parser.add_argument("--events_source", help="Indicates the way to obtain the events (google or generator)", type=str, default="google")
    parser.add_argument("--recommender_params", help="List of the features to use in recommender", nargs='+', default=["light price", "moon", "snow", "rain", "cloud"])
    parser.add_argument("--plot_results", help="A boolean parameter", action='store_true', default=True) # Plot results by default, if you don't want to plot results use --no_plot_results


    args = parser.parse_args()
    events_source = args.events_source
    recommender_params = args.recommender_params
    plot_results = args.plot_results

    # Call to the main function
    main(events_source, recommender_params, plot_results)
