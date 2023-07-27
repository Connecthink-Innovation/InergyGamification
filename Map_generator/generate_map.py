import random
import folium
import os
import json
import pandas as pd
import shutil
from typing import List

class MapGenerator():

    def __init__(self,):
        # Path to the input data folder
        self.input_data_path = os.path.join("Map_generator", "input_data")

        # File name for the classified nodes CSV file
        self.nodes_file_name = "classified_nodes.csv"

        # File name for the zone coordinates JSON file
        self.zone_coordinates_file_name = 'zone_coordinates.json'

        # Path to the output data folder
        self.output_data_path = os.path.join("Map_generator", "output_data")

        # File name for the output map HTML file
        self.output_file_name = "map.html"
        
        # A dictionary to store zone coordinates (to be read from the JSON file)
        self.zone_coordinates_dict = {}

        # A pandas DataFrame to store node zones information
        self.nodes_zones = pd.DataFrame() # < - id    type    ebox_id     lat     lon   zone


    #GET INPUT DATA
    def get_input_data(self):
        """
        Copy NodeClassifier csv output data to MapGenerator input data.

        Read the csv with the list of the nodes and their respective coordinates and
        the json containing the coordinates vertices of the zones of the municipality   

        Parameters:
            None

        Returns:
            None   
        """

        current_dir = os.getcwd()

        # Get the absolute paths of the "data" folders in the RSS_Spider and SkyInfo_Spider directories
        node_classifier_data_path = os.path.abspath(os.path.join(current_dir, "Node_classifier", "output_data"))

        # Check if the "input_data" folder exists, if not, create it
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copy all the files from the "output_data" folders of the NodeClassifier to the "input_data" folder of the MapGenerator
        self.copy_files_to_input_data(node_classifier_data_path)

        #Get csv of Nodes info
        csv_file = os.path.join(self.input_data_path, self.nodes_file_name)
        self.nodes_zones = pd.read_csv(csv_file) 

        # Get json of Zone clasifications for Canyelles
        json_file = os.path.join(self.input_data_path, self.zone_coordinates_file_name)
        with open(json_file) as f:
            self.zone_coordinates_dict = json.load(f)

    
    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        """
        Method to copy CSV files from the source directory to the "input_data" folder.

        Parameters:
            source_dir (str): The absolute path of the source directory containing the CSV files.

        Returns:
            None
        """
        for file_name in os.listdir(source_dir):
            src_file = os.path.join(source_dir, file_name)
            dst_file = os.path.join(self.input_data_path, file_name)
            if os.path.isfile(src_file):
                shutil.copy2(src_file, dst_file)
    
    
    
    def generate_bright_colors(self, n: int) -> List:
        """
        Returns a list of n dark colors for ploting the nodes in the map for each zone

        Parameters:
            n (int): The number of distinct dark colors required.
        Returns:
             List [str]: A list of n distinct dark colors in RGB format, represented as strings "rgb(r, g, b)".
        """
        colors = []
        
        # Predefined list of bright colors
        dark_colors = [
            (0, 0, 0),      # Black
            (25, 25, 25),
            (50, 50, 50),
            (75, 75, 75),
            (100, 100, 100),
            (125, 125, 125),
            (150, 150, 150),
            (175, 175, 175),
            (200, 200, 200),
            (25, 0, 0),
            (50, 0, 0),
            (75, 0, 0),
            (100, 0, 0),
            (125, 0, 0),
            (150, 0, 0),
            (175, 0, 0),
            (200, 0, 0),
            (0, 25, 0),
            (0, 50, 0),
            (0, 75, 0),
            (0, 100, 0),
            (0, 125, 0),
            (0, 150, 0),
            (0, 175, 0),
            (0, 200, 0),
            (0, 0, 25),
            (0, 0, 50),
            (0, 0, 75),
            (0, 0, 100),
            (0, 0, 125),
            (0, 0, 150)
        ]

        # Convert dark_colors to RGB format ("rgb(r, g, b)")
        rgb_colors = [f"rgb{color}" for color in dark_colors]
        
        if n > len(dark_colors):
            raise ValueError("Requested more colors for plotting than available in the predefined set. Please add more colors to the list bright_colors")
        
        # Select n distinct colors from the predefined list
        selected_colors = random.sample(rgb_colors, n)
        
        return selected_colors

    def assign_colors(self) -> dict:
        """
        Assigns a random color to each one of the zones for ploting
        
        Parameters:
            None
        Returns:
            dict: A dictionary mapping each zone to its assigned color in RGB format (as a string "rgb(r, g, b)").
        """
        
        colors = self.generate_bright_colors(len(self.zone_coordinates_dict))

        zone_colors = {}
        for key, color in zip(self.zone_coordinates_dict.keys(), colors):
            zone_colors[key] = color

        return zone_colors
    
    def plot_coordinates_on_map_zones(self, map, zone_colors: dict):
        """
        Returns a map object with the library Folium and plot all the nodes.

        Parameters:
            map (Folium.Map): The map object on which the nodes' coordinates will be plotted.
            zone_colors (dict): A dictionary that maps each zone to its assigned color in RGB format (as a string "rgb(r, g, b)").

        Returns:
            Folium.Map: The map object with the nodes' coordinates plotted.
        """

        # Group the data of the nodes by zone
        groups = self.nodes_zones.groupby("zone")

        # Iterate over each group (zone) and their respective node coordinates
        for zone, df in groups:

            coordinates = df[["lat", "lon"]].values

            # For each set of coordinates, add a CircleMarker to the map
            for coord in coordinates:
                folium.CircleMarker(
                    location=coord,
                    radius=0.2,  # Adjust the radius to make the circles smaller
                    color=zone_colors[zone], # Assign the corresponding color to the zone
                    fill=True,
                    fill_color=zone_colors[zone], # Assign the corresponding fill color to the zone
                    fill_opacity=1.0,
                ).add_to(map) # Add the marker to the map
        
        return map

    def generate_html_map(self) -> None:
        """
        Stores in a .html the map with the ploted nodes

        Parameters:
            None

        Returns:
            None
        """

        print("Generating map...")

        # Get a reference for the map:
        map_reference = self.nodes_zones.loc[2211][["lat", "lon"]].values

        # Generate the colors:
        zone_colors = self.assign_colors()

        map = folium.Map(map_reference, zoom_start=14)

        # Plot the coordinates of nodes on the map using zone_colors
        self.map = self.plot_coordinates_on_map_zones(map, zone_colors)


    #SAVE OUTPUT DATA
    def save_output_data(self,):
        """
        Saves the generated interactive map as an HTML file.

        Parameters:
            None
        
        Returns:
            None
        """

        dst_file = os.path.join(self.output_data_path, self.output_file_name)
        self.map.save(dst_file)
        print("html map generated in the file map.html")

# Temporal main:
map_generator = MapGenerator()
map_generator.get_input_data()
map_generator.generate_html_map()
map_generator.save_output_data()




    



