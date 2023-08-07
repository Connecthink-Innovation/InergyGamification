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
    def __init__(self, mode):
        """
        Constructor to set up input and output data paths, 
        and initialize a list to store real intensity vs recommended plots.
        """
        
        #Mode debug or prod
        self.mode = mode 

        # Set up input and output data paths
        self.input_data_path = os.path.join("Visualizer", "input_data")
        self.output_data_path = os.path.join("Visualizer", "output_data")

        # List to store real intensity vs recommended plots
        self.real_intensity_vs_recommended_plots = []

        # Obect to store zone savings plot
        self.zone_intensity_savings_plot = None
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

    def visualize_real_intensity_vs_recommended_individual(self, type="bar"):
        """
        Method to visualize real intensity vs recommended intensity for each zone and store the plots.

        Parameters:
            None

        Returns:
            None
        """

        # Loop through each "recommended" CSV file in the input_data_path
        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv") and "recommended" in file_name:
                # Extract the zone from the file name
                zone = (os.path.splitext(file_name)[0]).split("_intensity_")[1]

                #Read the csv and save DataFrame in a variable
                file_path = os.path.join(self.input_data_path, file_name)
                df = pd.read_csv(file_path)

                # Combine 'Date' and 'Hour' columns to create a new 'datetime' column
                df['datetime'] = df['Date'] + ' ' + df['Hour']

                # Get today's and tomorrow's date in the format 'YYYY-MM-DD'
                if self.mode == "prod":
                    today = datetime.now().strftime('%Y-%m-%d')
                    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

                if self.mode == "debug":
                    today = "2023-06-26"
                    tomorrow = "2023-06-27"

                # Set plot style
                sns.set(style='whitegrid')

                if type=="bar":
                    # Create a bar plot using Seaborn
                    fig, ax = plt.subplots(figsize=(10, 6))
                    sns.barplot(data=df, x='datetime', y='real_intensity', color='red', label='Real Intensity')
                    sns.barplot(data=df, x='datetime', y='recommended_intensity', color='green', label='Recommended Intensity')

                else:
                    # Create a scatter plot using Seaborn
                    fig, ax = plt.subplots(figsize=(10, 6))
                    sns.lineplot(data=df, x='datetime', y='real_intensity', color='red', label='Real Intensity', marker='o')
                    sns.lineplot(data=df, x='datetime', y='recommended_intensity', color='green', label='Recommended Intensity', marker='o')
                
                # Set axis labels
                plt.xlabel('Date and Hour')
                plt.ylabel('Intensity')
                # Set plot title
                plt.title('Real Intensity vs. Recommended Intensity')
                # Rotate the x-axis labels for better readability (set rotation to 90 degrees)
                plt.xticks(rotation=90, ha='center')
                # Add today's and tomorrow's date along with the zone to the legend
                plt.legend(title=f'Date: {today} -- {tomorrow}\nZone: {zone}', bbox_to_anchor=(1.02, 1), loc='upper left')
                
                # Save the plot and its corresponding zone in the list
                self.real_intensity_vs_recommended_plots.append((fig, zone))
                #plt.show()                   


    def visualize_zone_intensity_savings(self,):

        # Loop through each "savings" CSV file in the input_data_path
        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv") and "savings" in file_name:
                 #Read the csv and save DataFrame in a variable
                file_path = os.path.join(self.input_data_path, file_name)
                df = pd.read_csv(file_path)

                # Set the style of seaborn (optional, just for aesthetics)
                sns.set(style="whitegrid")
                
                # Create the chart using seaborn
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.barplot(x='zone', y='zone_savings', data=df)

                # Rotate the x-axis labels to make them readable
                plt.xticks(rotation=45, ha='right')

                # Set the title and labels of the axes
                plt.title('Percentage of savings by area')
                plt.xlabel('Zone')
                plt.ylabel('Savings (%)')

                # Save the plot in the variable 'zone_intensity_savings_plot'
                self.zone_intensity_savings_plot = fig 
                
                # Show the plot
                #plt.tight_layout()
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

        # Save zone intensity savings plot  
        fig = self.zone_intensity_savings_plot

        # Generate the file name for saving the plot
        file_name = f'zone_intensity_savings.png'
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
    
    # Visualize real intensity vs recommended intensity for each zone/iluminaire
    visualizer.visualize_real_intensity_vs_recommended_individual(type="bar")

    # Visualize intensity savings for each zone
    visualizer.visualize_zone_intensity_savings()

    # Save the generated plots to the output_data folder
    visualizer.save_output_data()

# Call the function to run the visualizer
run_visualizer()

