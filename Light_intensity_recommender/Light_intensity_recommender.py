import os
import shutil
import requests

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class LightIntensityRecommender:
    def __init__(self):
        self.input_data_path = os.path.join("Light_intensity_recommender", "input_data")
        self.output_data_path = os.path.join("Light_intensity_recommender", "output_data")

        self.df = pd.DataFrame()

    #GET INPUT DATA
    def get_input_data(self):
        current_dir = os.getcwd()

        # Obtener las rutas absolutas de las carpetas "data" en los directorios RSS_Spider y SkyInfo_Spider
        preprocessed_data_path = os.path.abspath(os.path.join(current_dir, "Preprocessor", "output_data"))

        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copiar todos los archivos de las carpetas "output_data" del Preprocessor a la carpeta "input_data" del Light Intensity Recommender
        self.copy_files_to_input_data(preprocessed_data_path)

    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

    
    #GET LIGHT PRICE
    def get_light_price_series(self):
        json_light_prices = requests.get("https://api.preciodelaluz.org/v1/prices/all?zone=PCB")
        return json_light_prices
    

    #CALCULATE RECOMMENDED LIGHT INTENSITY
    def calculate_recommended_light_intensity(self):
        csv_file = os.path.join(self.input_data_path, "processed_data.csv")
        df = pd.read_csv(csv_file)


        for index, row in df.iterrows():
            intensity = 100 #Initial intensity
            
            #Needs artificial light ?
            if not row["needs_artif_light"]:
                intensity = 0
            else:
                #Light price are high?
                if row["upper_light_price_mean"]:
                    intensity += -5

                #Is night?
                if row["is_night"]:
                    #We can reduce intensity depending on the moon ilumination - phase
                    intensity += -row["moon_illumination_percent"]*0.33*row["moon_phase_mult"]

            #Adjust intensity according to weather data
            if "snow" in row["condition"]:
                #Mirar cuantos dias lleva nevando!
                # ++
                pass

            if "rain" in row["condition"]:
                #row["precip_percent"] 
                #row["precip_mm"]
                # ++
                pass

            if "cloud" in row["condition"]:
                #row["cloud_cover_percent"]
                # ++
                pass

            #Temp module ++1% 
            #Humidity module ++1%
            #Pressure module ++1%
                
                

                
                

                

        # Guardar
        self.df = df

    #CALCULATE ENERGY SAVING
    def calculate_energy_savings(self,):

        self.df['savings'] = self.df.apply(self.energy_savings_formula, axis=1)

    # >> UTILS CES
    def energy_savings_formula(self, row):
        pass

    #SAVE OUTPUT DATA
    def save_output_data(self):
        file_name = "recommended_light_intensity.csv"
        dst_file = os.path.join(self.output_data_path, file_name)
        self.df.to_csv(dst_file, index=False)   

#test
light_intensity_recommender = LightIntensityRecommender()
light_intensity_recommender.get_input_data()
light_intensity_recommender.calculate_recommended_light_intensity()
light_intensity_recommender.calculate_energy_savings()
light_intensity_recommender.save_output_data()