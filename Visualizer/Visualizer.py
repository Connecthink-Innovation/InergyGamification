import seaborn as sns
import matplotlib.pyplot as plt

import pandas as pd
import os
import shutil
from datetime import datetime, timedelta

class Visualizer:
    """
    A class for visualizing real intensity vs recommended intensity data 
    and saving the plots as images.
    """
    def __init__(self):
        """
        Constructor to set up input and output data paths, 
        and initialize a list to store real intensity vs recommended plots.
        """

        # Set up input and output data paths
        self.input_data_path = os.path.join("Visualizer", "input_data")
        self.output_data_path = os.path.join("Visualizer", "output_data")

        # List to store real intensity vs recommended plots
        self.real_intensity_vs_recommended_plots = []

    # GET INPUT DATA
    def get_input_data(self):
        """
        Method to get input data from the 
        "Light_intensity_recommender/output_data" folder.

        Parameters:
            None

        Returns:
            None
        """
        
        current_dir = os.getcwd()

        # Get absolute paths of the "output_data" folders in the Light_intensity_recommender directory
        light_intensity_recommender_data_path = os.path.abspath(os.path.join(current_dir, "Light_intensity_recommender", "output_data"))

        # Check if the "input_data" folder exists; if not, create it
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copy all files from the "output_data" folders of Light_intensity_recommender to the "input_data" folder of Visualizer
        self.copy_files_to_input_data(light_intensity_recommender_data_path)

    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        """
        Method to copy CSV files from the source directory to the "input_data" folder.

        Parameters:
            source_dir (str): The path of the source directory containing CSV files to be copied.

        Returns:
            None
        """

        # Copy CSV files from the source directory to the input_data_path
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

    def visualize_real_intensity_vs_recommended(self,):
        """
        Method to visualize real intensity vs recommended intensity for each zone and store the plots.

        Parameters:
            None

        Returns:
            None
        """

        # Loop through each CSV file in the input_data_path
        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv"):
                # Extract the zone from the file name
                zone = (os.path.splitext(file_name)[0]).split("_intensity_")[1]

                #Read the csv and save DataFrame in a variable
                file_path = os.path.join(self.input_data_path, file_name)
                df = pd.read_csv(file_path)

                # Get today's and tomorrow's date in the format 'YYYY-MM-DD'
                today = datetime.now().strftime('%Y-%m-%d')
                tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

                # Create a bar plot using Seaborn
                sns.set(style='whitegrid')
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.barplot(data=df, x='Hour', y='real_intensity', color='red', label='Real Intensity')
                sns.barplot(data=df, x='Hour', y='recommended_intensity', color='green', label='Recommended Intensity')
                plt.xlabel('Hour')
                plt.ylabel('Intensity')
                plt.title('Real Intensity vs. Recommended Intensity')
                # Rotate the x-axis labels for better readability
                plt.xticks(rotation=45)
                # Add today's and tomorrow's date along with the zone to the legend
                plt.legend(title=f'Date: {today} -- {tomorrow}\nZone: {zone}')
                
                # Save the plot and its corresponding zone in the list
                self.real_intensity_vs_recommended_plots.append((fig, zone))
                #plt.show()

    #SAVE OUTPUT DATA
    def save_output_data(self):
        """
        Method to save the generated plots as images in the "output_data" folder.

        Parameters:
            None

        Returns:
            None       
        """

        # Check if the "output_data" folder exists; if not, create it
        if not os.path.exists(self.output_data_path):
            os.makedirs(self.output_data_path)

        # Iterate through the real intensity vs recommended plots list
        for real_intensity_vs_recommended_plot in self.real_intensity_vs_recommended_plots:

            fig = real_intensity_vs_recommended_plot[0]
            zone = real_intensity_vs_recommended_plot[1]

            # Generate the file name for saving the plot
            file_name = f'real_intensity_vs_recommended_plot_{zone}.png'
            dst_file = os.path.join(self.output_data_path, file_name)
            
            # Save the plot as an image file
            fig.savefig(dst_file, dpi=300, bbox_inches='tight')


#test

def run_visualizer():
    """
    Function to execute the visualizer pipeline.
    """
    
    # Create an instance of the Visualizer class
    visualizer = Visualizer()

    # Get input data from the Light_intensity_recommender output_data folder
    visualizer.get_input_data()
    
    # Visualize real intensity vs recommended intensity for each zone
    visualizer.visualize_real_intensity_vs_recommended()

    # Save the generated plots to the output_data folder
    visualizer.save_output_data()

# Call the function to run the visualizer
run_visualizer()

