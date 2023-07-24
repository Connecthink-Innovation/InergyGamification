import os
import shutil
import requests
import argparse

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class LightIntensityRecommender:
    def __init__(self):
        self.input_data_path = os.path.join("Light_intensity_recommender", "input_data")
        self.output_data_path = os.path.join("Light_intensity_recommender", "output_data")

        self.df_list = []

    #GET INPUT DATA
    def get_input_data(self):
        current_dir = os.getcwd()

        # Obtener las rutas absolutas de las carpetas "output_data" en el directorio Preprocessor 
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
    

    #CALCULATE RECOMMENDED LIGHT INTENSITY
    def calculate_recommended_light_intensity(self, args):
    
        csv_file = os.path.join(self.input_data_path, "processed_data_previous.csv")
        df_previous = pd.read_csv(csv_file)

        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv") and not "previous" in file_name:

                zone = (os.path.splitext(file_name)[0]).split("_next_")[1]
                file_path = os.path.join(self.input_data_path, file_name)
                df_next = pd.read_csv(file_path)

                real_intensity_list = []
                recommended_intensity_list = []
                
                for index, row in df_next.iterrows():
                    recommended_intensity = 100 #Initial recommended intensity
                    real_intensity = float(100)
                    
                    #Needs artificial light ?
                    if not row["needs_artif_light"]:
                        real_intensity = float(0)
                        recommended_intensity = 0

                    else:
                        
                        if pd.notna(row["events_titles"]):
                            recommended_intensity += 20

                        #Light price are high?
                        if row["upper_light_price_mean"]:
                            if "light price" in args:
                                price_score = self.calc_price_score(df_next, index, row)
                                recommended_intensity -= 10*price_score

                        #Is night?
                        if row["is_night"]:
                            #We can reduce intensity depending on the moon ilumination - phase
                            if "moon" in args:
                                recommended_intensity -= row["moon_illumination_percent"]*0.33*row["moon_phase_mult"]
                        
                        #Adjust intensity according to weather data
                        if "snow" in args:
                            df1 = df_previous[["condition", "temp_celsius", "Year", "Month", "Day", "Hour"]]
                            df2 = df_next[["condition", "temp_celsius", "Year", "Month", "Day", "Hour"]]
                            df_condition = pd.concat([df1, df2])
                            df_condition = df_condition.reset_index(drop=True)

                            snow_score = self.calc_snow_score(df_condition, index, row)
                            recommended_intensity -= 10*snow_score

                        if "rain" in args:
                            rain_score = self.calc_rain_score(df_next, index, row)
                            recommended_intensity += 10*rain_score

                        if "cloud" in args:
                            cloud_score = self.calc_cloud_score(df_next, index, row)
                            recommended_intensity += 10*cloud_score

                    #Temp module ++1% 
                    #Humidity module ++1%
                    #Pressure module ++1%

                    #Adjust intensity according to events data
                    #
                    #
                    #
                    #
                    if recommended_intensity > 100: 
                        recommended_intensity = 100

                    recommended_intensity_list.append(recommended_intensity)
                    real_intensity_list.append(real_intensity)

                # Save intensity and dataframe
                df_next['real_intensity'] = real_intensity_list
                df_next['recommended_intensity'] = recommended_intensity_list
                self.df_list.append((df_next, zone))
                
    def calc_price_score(self, df, index, row):

        mean_light_price = df["light_price_kwh"].mean()
        min_light_price = df["light_price_kwh"].min()
        max_light_price = df["light_price_kwh"].max()

        diff_max = max_light_price - mean_light_price

        diff_act = row["light_price_kwh"] - mean_light_price

        price_score = diff_act / diff_max
        
        if price_score < 0:
            price_score = 0

        return price_score

    def calc_snow_score(self, df, index, row):

        snow_is_decisive = False

        start_index = max(0, index - 72)  # Starting index of slider range
        end_index = index - 1  # end index of slider range
        
        # Check if any of the rows contain the substring "snow" in the column "condition"
        condition_contains_snow = df.loc[start_index:end_index, 'condition'].str.contains('snow').any()

        

        if condition_contains_snow:
            #Get index from the hour it snowed
            first_snow_index = df.loc[start_index:end_index, 'condition'].str.contains('snow').idxmax()

            #Average temperatures from the time it snowed to the current time
            mean_temp = df.loc[first_snow_index:end_index, 'temp_celsius'].mean()

            #If mean temperature is < 10, It means that there is snow on the ground
            if mean_temp < 10:
                snow_is_decisive = True

        # Assign a score
        if snow_is_decisive:
            score = 1 ##################### ! ! ! #####################
        
        else:
            score = 0 ##################### ! ! ! #####################

      
        """
        Tambien deberiamos mirar tema de cuantos dias a nevado.  .  .  .   .  .  .  .  .     .    .  . . . .. 
        """

        return score
    

    def calc_rain_score(self, df, index, row):
        
        precip = row["precip_mm"]

        if precip < 0.5 : # Very light rain
            score = 0   ##################### ! ! ! #####################
        
        elif precip < 2.5: # Light rain
            score = 0.2 ##################### ! ! ! #####################

        elif precip < 7.6: # Moderate rain
            score = 0.4 ##################### ! ! ! #####################

        elif precip < 15: # Moderately heavy rain
            score = 0.6 ##################### ! ! ! #####################

        elif precip < 30: # Heavy rain
            score = 0.8 ##################### ! ! ! #####################

        else: #Very heavy rain
            score = 1   ##################### ! ! ! #####################

        
        #row["precip_percent"]  <- check?
    
        """
        #Lluvia muy ligera: Menos de 0.5 mm/h
        #Lluvia ligera: 0.5 mm/h - 2.5 mm/h
        #Lluvia moderada: 2.5 mm/h - 7.6 mm/h
        #Lluvia moderadamente intensa: 7.6 mm/h - 15 mm/h
        #Lluvia intensa: 15 mm/h - 30 mm/h
        #Lluvia muy intensa: Más de 30 mm/h
        """ 

        return score
        
    
    def calc_cloud_score(self, df, index, row):

        score = row["cloud_cover_percent"] * 0.01 #Calculate cloud score
                
        return score
    

    #CALCULATE ENERGY SAVING
    def calculate_energy_savings(self,):
        df_list_temp = []
        for df_zone in self.df_list:

            df = df_zone[0]
            zone = df_zone[1]

            df['savings'] = df.apply(self.energy_savings_formula, axis=1)

            df_list_temp.append((df, zone))
        
        self.df_list = df_list_temp

    # >> UTILS CES
    def energy_savings_formula(self, row):
        pass

    #SAVE OUTPUT DATA
    def save_output_data(self):

        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.output_data_path):
            os.makedirs(self.output_data_path)

        #Iter individual df zones list
        for df_zone in self.df_list:

            df = df_zone[0]
            zone = df_zone[1]

            file_name = f'recommended_light_intensity_{zone}.csv'
            dst_file = os.path.join(self.output_data_path, file_name)
            df.to_csv(dst_file, index=False)   

#test

def run_intensity_recommender():
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", help="List of the municipalities the user wants to preprocess", nargs='+', default=["light price", "moon", "snow", "rain", "cloud", "events"])
    
    args = parser.parse_args()
    params = args.params

    light_intensity_recommender = LightIntensityRecommender()
    light_intensity_recommender.get_input_data()
    light_intensity_recommender.calculate_recommended_light_intensity(params)
    light_intensity_recommender.calculate_energy_savings()
    light_intensity_recommender.save_output_data()

run_intensity_recommender()

