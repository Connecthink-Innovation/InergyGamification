import os
import pandas as pd
import json
import shutil

class NodeClassifier():

    def __init__(self,):
        # Paths for input and output data folders, and file names
        self.input_data_path = os.path.join("Node_classifier", "input_data")
        self.nodes_file_name = "nodes.csv"
        self.zone_coordinates_file_name = 'zone_coordinates.json'

        self.output_data_path = os.path.join("Node_classifier", "output_data")
        self.output_file_name = "classified_nodes.csv"

        # DataFrames and dictionary to store data
        self.df = pd.DataFrame()        # < - id    type    ebox_id     lat     lon    
        self.zone_coordinates_dict = {} # < -  { 
                                                    # {zone_name":
                                                    #      {
                                                    #           tr: top right coordinate
                                                    #           tl: top left coordinate
                                                    #           lr: lower right coordinate
                                                    #           ll: lower left coordinate
                                                    #      }
                                                    #      ...
                                                    #      ...
                                                    # }
                                        #      }

    def get_input_data(self,) -> None:
        """
        Reads the CSV with the list of nodes and their respective coordinates and
        the JSON containing the coordinates vertices of the zones of the municipality.

        Parameters:
            None

        Returns:
            None
        """

        #Get csv of Nodes info
        csv_file = os.path.join(self.input_data_path, self.nodes_file_name)
        self.df = pd.read_csv(csv_file) 

        # Get JSON of Zone clasifications for Canyelles
        json_file = os.path.join(self.input_data_path, self.zone_coordinates_file_name)
        with open(json_file) as f:
            self.zone_coordinates_dict = json.load(f)
        
        # Strucure of the zone_coordinates_dict:
            # tr: top right coordinate
            # tl: top left coordinate
            # lr: lower right coordinate
            # ll: lower left coordinate

    def create_coordinates_dict(self,row: pd.Series) -> dict:
        """
        Helper function that gets a row of the dataframe and extracts the lat and lon into a dictionary.
        This is used to create a new column in the nodes dataframe that contains the lon and lat all in 
        one column.

        Parameters:
            row (pd.Series): A row of the DataFrame containing lat and lon information.

        Returns:
            dict: A dictionary with 'lat' and 'lon' keys containing the respective latitudinal and longitudinal values.
        """

        return {'lat': row['lat'], 'lon': row['lon']}
    
    def device_in_zone(self, zone_coordinates: dict, device_coordinates: dict) -> bool:
        """
        For a certain zone of the municipality returns whether or not the device coordinate are in this
        zone

        Parameters:
            zone_coordinates (dict): A dictionary containing the four vertices coordinates of the zone.
            device_coordinates (dict): A dictionary containing the lat and lon of the device.

        Returns:
            bool: True if the device coordinates are in the zone, False otherwise.
        """

        # Extract zone coordinates
        tr = zone_coordinates["tr"]
        tl = zone_coordinates["tl"]
        lr = zone_coordinates["lr"]
        ll = zone_coordinates["ll"]

        # Define ranges of latitude and longitude for the zone
        lat_max = tl["lat"]
        lat_min = ll["lat"]
        lon_min = tl["lon"]
        lon_max = tr["lon"]

        # Extract device coordinates
        lat = device_coordinates["lat"]
        lon = device_coordinates["lon"]

        # Check if the coordinates of the device are in the zone
        if ((lat_min <= lat) & (lat <= lat_max)) & ((lon_min <= lon) & (lon <= lon_max)):
            return True
        else:
            return False

    def zone_classificator(self, device_coordinates: dict) -> str:
        """
        Returns the zone of the municipality in which the device_coordinates are located. If there is no match the function
        returns 'Unknown'

        Parameters:
            device_coordinates (dict): A dictionary containing the lat and lon of the device.

        Returns:
            str: The zone name where the device is located or 'Unknown' if no match is found.
        """

        zone_out = "Unknown"

        for zone in self.zone_coordinates_dict.keys():
            if zone == "south":
                # First, check if the device is in the south rectangle but not in the tourist zone or football field
                if (
                        (self.device_in_zone(self.zone_coordinates_dict["south"], device_coordinates) &
                        (self.device_in_zone(self.zone_coordinates_dict["field2"], device_coordinates) == False) &
                        (self.device_in_zone(self.zone_coordinates_dict["turistic"], device_coordinates) == False))
                    ):
                    zone_out = "south"

            else: # Any other zone
                if self.device_in_zone(self.zone_coordinates_dict[zone], device_coordinates):
                    zone_out = zone

        # If we have not returned anything return "Unknown"
        return zone_out

    def classify_nodes(self,):
        """
        Creates a new column in the nodes dataframe that contains the zone 
        in which we have classified the node.

        Parameters:
            None

        Returns:
            None
        """

        # Create a new column with coordinates as a dictionary
        self.df["coordinates"] = self.df.apply(lambda row: self.create_coordinates_dict(row), axis=1)

        # Apply the zone_classificator to classify nodes
        self.df["zone"] = self.df["coordinates"].apply(lambda coord: self.zone_classificator(coord))

        # Keep only the lights in the DataFrame
        self.df = self.df.loc[self.df["type"] == "light"]

    #SAVE OUTPUT DATA
    def save_output_data(self):
        """
        Saves the classified nodes DataFrame and zone_coordinates.json to the output_data folder.

        Parameters:
            None

        Returns:
            None
        """

        # Save the classified nodes DataFrame
        dst_file = os.path.join(self.output_data_path, self.output_file_name)
        self.df.to_csv(dst_file, index=False)

        # Copy zone_coordinates.json to the output_data folder
        src_file = os.path.join(self.input_data_path, self.zone_coordinates_file_name)
        dst_file = os.path.join(self.output_data_path, self.zone_coordinates_file_name)
        shutil.copy2(src_file, dst_file)

#DEBUG
def main():
    print("Classifying nodes in their relevant zone...")
    node_clasif = NodeClassifier()
    node_clasif.get_input_data()
    node_clasif.classify_nodes()
    node_clasif.save_output_data()
    print("Classified nodes.\n")

#main()