import os
import pandas as pd
import json

class NodeClassifier():

    def __init__(self,):
        self.input_data_path = os.path.join("Node_classifier", "input_data")
        self.nodes_file_name = "nodes.csv"
        self.zone_coordinates_file_name = 'zone_coordinates.json'

        self.output_data_path = os.path.join("Node_classifier", "output_data")
        self.output_file_name = "classified_nodes.csv"

        
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



    def get_input_data(self,):

        #Get csv of Nodes info
        csv_file = os.path.join(self.input_data_path, self.nodes_file_name)
        self.df = pd.read_csv(csv_file) 

        # Get json of Zone clasifications for Canyelles
        json_file = os.path.join(self.input_data_path, self.zone_coordinates_file_name)
        with open(json_file) as f:
            self.zone_coordinates_dict = json.load(f)

            # tr: top right coordinate
            # tl: top left coordinate
            # lr: lower right coordinate
            # ll: lower left coordinate


    def classify_nodes(self,):
        # Create coordinates
        self.df["coordinates"] = self.df.apply(lambda row: self.create_coordinates_dict(row), axis=1)

        # Apply the zone_classificator:
        self.df["zone"] = self.df["coordinates"].apply(lambda coord: self.zone_classificator(coord))

        # Keep only the lights:
        self.df = self.df.loc[self.df["type"] == "light"]


    def create_coordinates_dict(self,row):

        return {'lat': row['lat'], 'lon': row['lon']}
    

    def zone_classificator(self, device_coordinates: dict) -> str:
        zone_out = "Unknown"

        for zone in self.zone_coordinates_dict.keys():
            if zone == "south":
                # First we have to check if the device is in the south rectangle but at the same time not in the turistic and not in the football field
                if (

                        (self.device_in_zone(self.zone_coordinates_dict["south"], device_coordinates) &

                        (self.device_in_zone(self.zone_coordinates_dict["field2"], device_coordinates) == False) &

                        (self.device_in_zone(self.zone_coordinates_dict["turistic"], device_coordinates) == False))

                    ):

                    zone_out = "south"

            else: # any other zone
                if self.device_in_zone(self.zone_coordinates_dict[zone], device_coordinates):

                    zone_out = zone

        # If we have not returned anything return "Unknown"
        return zone_out
    

    def device_in_zone(self, zone_coordinates: dict, device_coordinates: dict) -> bool:

        # Zone coordinates

        tr = zone_coordinates["tr"]

        tl = zone_coordinates["tl"]

        lr = zone_coordinates["lr"]

        ll = zone_coordinates["ll"]

        # Ranges of latitude and longitude of the zone:

        lat_max = tl["lat"]

        lat_min = ll["lat"]

        lon_min = tl["lon"]

        lon_max = tr["lon"]

        # Device coordinates

        lat = device_coordinates["lat"]

        lon = device_coordinates["lon"]

        # Check if the coordinates of the dedvice are in the zone

        if ((lat_min <= lat) & (lat <= lat_max)) & ((lon_min <= lon) & (lon <= lon_max)):

            return True

        else:

            return False
        
    

    #SAVE OUTPUT DATA
    def save_output_data(self):

        dst_file = os.path.join(self.output_data_path, self.output_file_name)
        self.df.to_csv(dst_file, index=False)


#DEBUG
node_clasif = NodeClassifier()
node_clasif.get_input_data()
node_clasif.classify_nodes()
node_clasif.save_output_data()