#---------------------------------------------------------------------------------------------
import os

# Get the path to the directory of the current file
current_file_path = os.path.abspath(__file__)

# Get the path to the root directory of the project
project_root = os.path.dirname(os.path.dirname(current_file_path))

# You can now import modules from calculated routes
import sys
sys.path.append(project_root)  # Add the root directory to the PYTHONPATH
#---------------------------------------------------------------------------------------------

#skyinfo imports
from SkyInfo_Spiders.SkyInfo_Spiders.spiders import meteo, moon_phases, moonrise_moonset, sunrise_sunset

#lightprice imports
from LightPrice_Spiders.LightPrice_Spiders.spiders import light_price

#events imports
from RSS_Spiders.RSS_Spiders.spiders import google_events
from Event_generator import event_generator

#node classifier imports
from Node_classifier import node_classifier

#preprocessor imports
from Preprocessor.Preprocessor import Preprocessor

#recommender imports
from Light_intensity_recommender.Light_intensity_recommender import LightIntensityRecommender

#visualizer imports
from Visualizer.Visualizer import Visualizer

#utils
import argparse 
import multiprocessing

class Gamification():
    def __init__(self, mode, events_source, recommender_params, plots_results):
        #Mode debug or prod
        self.mode = mode

        # Indicates the source of the events to be used
        self.events_source = events_source    

        # Indicates the parameters used in the light intensity recommender
        self.recommender_params = recommender_params

        # Display results if True
        self.plot_results = plots_results  


    def run_scrapies(self,):

        processes = []

        processes.append(multiprocessing.Process(target=meteo.run_scrapy, args=(self.mode, project_root)))
        processes.append(multiprocessing.Process(target=moon_phases.run_spider, args=(self.mode, project_root)))
        processes.append(multiprocessing.Process(target=moonrise_moonset.run_spider, args=(self.mode, project_root)))
        processes.append(multiprocessing.Process(target=sunrise_sunset.run_spider, args=(self.mode, project_root)))
        processes.append(multiprocessing.Process(target=light_price.run_spider, args=(self.mode, project_root)))

        if self.events_source == "google":
            processes.append(multiprocessing.Process(target=google_events.run_spider, args=(project_root,)))
        else:
            processes.append(multiprocessing.Process(target=event_generator.main, args=(project_root,)))

        for process in processes:
            process.start()

        for process in processes:
            process.join()

    def run_node_classifier(self,):
        node_classifier.main()


    def run_preprocessor(self,):
        preprocessor = Preprocessor(self.mode, self.events_source)
        preprocessor.get_input_data()
        preprocessor.preprocess_data()
        preprocessor.save_output_data()


    def run_light_intensity_recommender(self,):

        light_intensity_recommender = LightIntensityRecommender()
        light_intensity_recommender.get_input_data()
        light_intensity_recommender.calculate_recommended_light_intensity(self.recommender_params)
        light_intensity_recommender.calculate_intensity_savings()
        light_intensity_recommender.calculate_co2_consumption()
        light_intensity_recommender.save_output_data()

    def run_visualizer(self,):
        if self.plot_results:
            visualizer = Visualizer(self.mode)
            visualizer.get_input_data()
            visualizer.visualize_real_intensity_vs_recommended_individual(type="bar")
            visualizer.visualize_zone_intensity_savings()
            visualizer.save_output_data()

def main(mode, events_source, recommender_params, plot_results):
    gamification = Gamification(mode, events_source, recommender_params, plot_results)
    #gamification.run_scrapies()
    #gamification.run_node_classifier()
    gamification.run_preprocessor()
    gamification.run_light_intensity_recommender()
    gamification.run_visualizer()

if __name__ == "__main__":
    # Setting the command line argument to customize the recommender    
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="Indicates the executation mode (debug or prod)", type=str, default="prod")
    parser.add_argument("--events_source", help="Indicates the way to obtain the events (google or generator)", type=str, default="google")
    parser.add_argument("--recommender_params", help="List of the features to use in recommender", nargs='+', default=["light price", "moon", "snow", "rain", "cloud"])
    parser.add_argument("--plot_results", help="A boolean parameter", action='store_true', default=True) # Plot results by default, if you don't want to plot results use --no_plot_results


    args = parser.parse_args()
    mode = args.mode
    events_source = args.events_source
    recommender_params = args.recommender_params
    plot_results = args.plot_results

    # Call to the main function
    main(mode, events_source, recommender_params, plot_results)
