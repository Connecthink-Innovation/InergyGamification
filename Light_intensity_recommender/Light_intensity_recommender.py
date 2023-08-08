import os
import shutil
import requests
import argparse

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class LightIntensityRecommender:
    """
    A class for recommending light intensity levels based on various factors and calculating energy savings.
    """

    def __init__(self):
        """
        Constructor to set up input and output data paths and initialize a list to store DataFrames.
        """
        self.input_data_path = os.path.join("Light_intensity_recommender", "input_data")
        self.output_data_path = os.path.join("Light_intensity_recommender", "output_data")

        self.df_list = []

    #GET INPUT DATA
    def get_input_data(self):
        """
        Method to get input data from the "Preprocessor/output_data" folder 
        and copy it to the "input_data" folder.
        
        Parameters:
            None

        Returns:
            None       
        """

        current_dir = os.getcwd()

        # Get absolute paths of the "output_data" folders in the Preprocessor directory
        preprocessed_data_path = os.path.abspath(os.path.join(current_dir, "Preprocessor", "output_data"))

        # Check if the "input_data" folder exists; if not, create it
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copy all files from the "output_data" folder of Preprocessor to the "input_data" folder of Light Intensity Recommender
        self.copy_files_to_input_data(preprocessed_data_path)

    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        """
        Method to copy CSV files from the source directory to the "input_data" folder.

        Parameters:
            source_dir (str): The absolute path of the source directory containing the CSV files to be copied.

        Returns:
            None    
        """

        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)
    

    #CALCULATE RECOMMENDED LIGHT INTENSITY
    def calculate_recommended_light_intensity(self, args):
        """
        Method to calculate recommended light intensity levels based on various factors.

        Parameters:
            args (list): A list of strings specifying the factors to consider for intensity calculation (e.g., ["moon", "rain", "cloud"]).

        Returns:
            None
        """

        # Read the previous processed_data_previous.csv file
        csv_file = os.path.join(self.input_data_path, "processed_data_previous.csv")
        df_previous = pd.read_csv(csv_file)

        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv") and not "previous" in file_name:
                # Extract the zone from the file name
                zone = (os.path.splitext(file_name)[0]).split("_next_")[1]
                file_path = os.path.join(self.input_data_path, file_name)
                df_next = pd.read_csv(file_path)

                # Create lists to append recommended intensity and explanation for all hours/luminaires
                real_intensity_list = []
                recommended_intensity_list = []
                recommended_intensity_explanation_list = []
                
                # Iterate over each row of the dataset (hour-zone)
                for index, row in df_next.iterrows():
                    recommended_intensity = 100 # Initiate recommended intensity
                    real_intensity = float(100) # Real intensity its 100
                    recommended_intensity_explanation = "" # Initiate recommended intensity explanation
                    
                    # Check if artificial light is needed
                    if not row["needs_artif_light"]:
                        real_intensity = float(0)
                        recommended_intensity = 0
                        recommended_intensity_explanation += f'No artificial light needed, recommended intensity: 0\n'

                    else:
                        # Check various parameters and adjust the recommended intensity accordingly                       
                        
                        # Check if there are events
                        if pd.notna(row["events_titles"]):
                            recommended_intensity += 20
                            recommended_intensity_explanation += f'There is an event in this zone and hour, recommended intensity increases by 20%\n'


                        # Check if light price is high and adjust the recommended intensity
                        if row["upper_light_price_mean"]:
                            if "light price" in args:
                                price_score = self.calc_price_score(df_next, index, row)
                                recommended_intensity -= 10*price_score
                                recommended_intensity_explanation += f'The price of light is high at this hour, recommended intensity decreases by {10*price_score}%\n'


                        # Check if it is night and adjust the recommended intensity based on moon illumination
                        if row["is_night"]:
                            #We can reduce intensity depending on the moon ilumination - phase
                            if "moon" in args:
                                moon_score = self.calc_moon_score(df_next, index, row)
                                recommended_intensity -= 10*moon_score
                                recommended_intensity_explanation += f'It is night and the moon is {row["moon_phase"]}, recommended intensity decreases by {10*moon_score}%\n'
                        
                        # Adjust intensity based on weather data (e.g., snow, rain, cloud)
                        if "snow" in args:
                            df1 = df_previous[["condition", "temp_celsius", "Year", "Month", "Day", "Hour"]]
                            df2 = df_next[["condition", "temp_celsius", "Year", "Month", "Day", "Hour"]]
                            df_condition = pd.concat([df1, df2])
                            df_condition = df_condition.reset_index(drop=True)

                            snow_score = self.calc_snow_score(df_condition, index+len(df1), row)
                            if snow_score:
                                recommended_intensity -= 10*snow_score
                                recommended_intensity_explanation += f'It has snowed the last few days and it affects the lighting of the streets, recommended intensity decreases by {10*snow_score}%\n'

                        if "rain" in args:
                            rain_score = self.calc_rain_score(df_next, index, row)
                            if rain_score:
                                recommended_intensity += 10*rain_score
                                recommended_intensity_explanation += f'It is raining {row["precip_mm"]} mm/h in this zone and hour, recommended intensity increases by {10*rain_score}%\n'

                        if "cloud" in args:
                            cloud_score = self.calc_cloud_score(df_next, index, row)
                            if cloud_score:
                                recommended_intensity += 10*cloud_score
                                recommended_intensity_explanation += f'The sky is {row["condition"]} in this zone and hour, recommended intensity increases by {10*cloud_score}%\n'

                    #Temp module ++1% 
                    #Humidity module ++1%
                    #Pressure module ++1%

                    if recommended_intensity > 100: 
                        recommended_intensity = 100

                    recommended_intensity_list.append(recommended_intensity)
                    real_intensity_list.append(real_intensity)
                    recommended_intensity_explanation_list.append(recommended_intensity_explanation)

                # Save intensity and dataframe
                df_next['real_intensity'] = real_intensity_list
                df_next['recommended_intensity'] = recommended_intensity_list
                df_next['recommended_intensity_explanation'] = recommended_intensity_explanation_list
                self.df_list.append((df_next, zone))
                
    def calc_price_score(self, df, index, row):
        """
        Method to calculate the price score for adjusting recommended intensity based on light price.

        Parameters:
            df (DataFrame): DataFrame containing the data for the current zone.
            index (int): Index of the current row in the DataFrame.
            row (Series): The current row of the DataFrame.

        Returns:
            float: The calculated price score.
        """

        mean_light_price = df["light_price_kwh"].mean()
        min_light_price = df["light_price_kwh"].min()
        max_light_price = df["light_price_kwh"].max()

        diff_max = max_light_price - mean_light_price

        diff_act = row["light_price_kwh"] - mean_light_price

        price_score = diff_act / diff_max
        
        if price_score < 0:
            price_score = 0

        return price_score
    
    def calc_moon_score(self, df, index, row):
        """
        Method to calculate the moon score for adjusting recommended intensity based on moon illumination.

        Parameters:
            df (DataFrame): DataFrame containing the data for the current zone.
            index (int): Index of the current row in the DataFrame.
            row (Series): The current row of the DataFrame.

        Returns:
            float: The calculated moon score.
        """

        return (row["moon_illumination_percent"]*0.33*row["moon_phase_mult"]) / 100

    def calc_snow_score(self, df, index, row):
        """
        Method to calculate the snow score for adjusting recommended intensity based on snow conditions.

        Parameters:
            df (DataFrame): DataFrame containing the data for the current zone.
            index (int): Index of the current row in the DataFrame.
            row (Series): The current row of the DataFrame.

        Returns:
            int: The calculated snow score (1 if snow is decisive, 0 otherwise).
        """

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
        """
        Method to calculate the rain score for adjusting recommended intensity based on rain conditions.

        Parameters:
            df (DataFrame): DataFrame containing the data for the current zone.
            index (int): Index of the current row in the DataFrame.
            row (Series): The current row of the DataFrame.

        Returns:
            float: The calculated rain score.
        """

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
        """
        Method to calculate the cloud score for adjusting recommended intensity based on cloud cover.

        Parameters:
            df (DataFrame): DataFrame containing the data for the current zone.
            index (int): Index of the current row in the DataFrame.
            row (Series): The current row of the DataFrame.

        Returns:
            float: The calculated cloud score.
        """
        try:
            score = row["cloud_cover_percent"] * 0.01 #Calculate cloud score
        
        except:
            score = 0
            
        return score
    

    #CALCULATE ENERGY SAVING
    def calculate_intensity_savings(self,):
        """
        Method to calculate the energy savings for each zone and hour based on the recommended light intensity.

        This method iterates over the list of DataFrames for each zone and applies the energy_savings_formula
        to calculate the energy savings for each row (hour) in the DataFrame. The calculated savings are then
        added as a new column 'savings' in the DataFrame.

        The updated DataFrames are stored in a temporary list and then assigned back to self.df_list.

        Parameters:
            None

        Returns:
            None
        """

        #Temporary list to save the modified zone/iluminaire df
        df_list_temp = []
        #Temporary list to save savings summary
        savings_summary_list = []

        #Loop through each zone df
        for df_zone in self.df_list:

            df = df_zone[0]
            zone = df_zone[1]

            # Calculate intensity savings (of each luminaire and each zone)
            individual_savings_list, zone_savings = self.intensity_savings_formula(df)  
            
            # Save savings for luminaire in that zone
            df["savings"] = individual_savings_list

            # Save savings for zone
            savings_summary_list.append((zone, zone_savings))

            # Save updated df in temporary list
            df_list_temp.append((df, zone))
        
        # Calculate savings for the entire municipality
        total_savings = [elem[1] for elem in savings_summary_list if elem]
        total_savings_sum = sum(total_savings)
        savings_summary_list.append(("total", total_savings_sum))

        # Update the original df list of the class
        self.df_list = df_list_temp

        # Create df attribute class to store the df of savings per zone and municipality
        self.df_savings_summary = pd.DataFrame(savings_summary_list, columns=["zone", "zone_savings"])

        

    # >> UTILS CIS
    def intensity_savings_formula(self, df):
        """
        Utility method to calculate energy savings based on the difference between recommended and real light intensity.

        This method is applied to each row (hour) in the DataFrame. It calculates the energy savings by taking the difference
        between the recommended light intensity and the real light intensity, and then multiplies it by a constant factor 
        representing energy savings per unit of light intensity saved.

        Parameters:
            row (Series): The current row of the DataFrame.

        Returns:
            float: The calculated energy savings for the current row.
        """

        individual_savings_list = []
        zone_savings = 0

        for index, row in df.iterrows():
            individual_savings = row["real_intensity"]-row["recommended_intensity"]
            individual_savings_list.append(individual_savings)

        
        zone_savings = sum(individual_savings_list)

        return individual_savings_list, zone_savings


    #CALCULATE CO2 SAVING
    def calculate_co2_consumption(self,):

        """
        Method to calculate real-recommended co2 levels based on real-recommended light intensity.

        Parameters:
            None

        Returns:
            None
        """

        #Loop through each zone df

        potencia_en_kw = 0.1 #de momento 
        factor_emision_co2 = 0.5 #de momento
        horas = 1 #de momento

        for i, df_zone in enumerate(self.df_list):

            df = df_zone[0]
            zone = df_zone[1]

            
            # Calculate CO2 consumption for real intensity
            df['real_CO2_consumption'] = df.apply(lambda row: self.co2_consumption_formula(1, row['real_intensity'], potencia_en_kw, horas, factor_emision_co2), axis=1)

            # Calculate CO2 consumption for recommended intensity
            df['recommended_CO2_consumption'] = df.apply(lambda row: self.co2_consumption_formula(1, row['recommended_intensity'], potencia_en_kw, horas, factor_emision_co2), axis=1)

            # Update the DataFrame in the list
            self.df_list[i] = (df, zone)


        #CO2_consumido = (potencia_en_kw * tiempo_encendido_en_horas) * factor_emision_CO2
        #donde potencia_en_kw hace referencia PowerAparent
        #donde PowerAparent hace referencia a sqrt((PowerActive)**2 + (PowerReactive)**2)

        #CO2_consumido_real = (potencia_en_kw * intensidad_real100% * tiempo_encendido_en_horas) * factor_emision_CO2
        #CO2_consumido_recomm = (potencia_en_kw * intensidad_recomm * tiempo_encendido_en_horas) * factor_emision_CO2

    def co2_consumption_formula(self, n_luminaires, intensity_percent, potencia_en_kw, horas, factor_emision_co2):
        """
        Method to calculate CO2 consumption based on real-recommended light intensity.

        Parameters:
            intensity (float): Intensity value (real or recommended).
            potencia_en_kw (float): Power in kilowatts.
            factor_emision_co2 (float): CO2 emission factor.

        Returns:
            float: The calculated CO2 consumption.
        """

        # Calculate CO2 consumption for a specific intensity
        intensity = intensity_percent/100
        CO2_consumido = n_luminaires * ((potencia_en_kw * intensity * horas) * factor_emision_co2)

        return CO2_consumido
    

    #CALCULATE CO2 SAVING
    def calculate_co2_savings(self,):
        #CO2_ahorrado = CO2_consumido_real - CO2_consumido_recomm
        pass
    
    # >> UTILS CCS
    def co2_savings_formula(self,):
        pass

    #SAVE OUTPUT DATA
    def save_output_data(self):
        """
        Method to save the output data (recommended light intensity and energy savings) to CSV files.

        This method iterates over the list of DataFrames for each zone, and saves the DataFrames to separate CSV files
        in the "output_data" folder. Each CSV file is named based on the zone and contains columns for the recommended
        light intensity, real light intensity, and energy savings.

        Parameters:
            None

        Returns:
            None
        """

        # Check if the "output_data" folder exists, if not, create it
        if not os.path.exists(self.output_data_path):
            os.makedirs(self.output_data_path)

        # Iter individual df zones list (save individual zone csv's)
        for df_zone in self.df_list:

            df = df_zone[0]
            zone = df_zone[1]

            file_name = f'recommended_light_intensity_{zone}.csv'
            dst_file = os.path.join(self.output_data_path, file_name)
            df.to_csv(dst_file, index=False)   
        
        # Save saving summary by zone csv
        file_name = f'savings_summary.csv'
        dst_file = os.path.join(self.output_data_path, file_name)
        self.df_savings_summary.to_csv(dst_file, index=False)




#test

def run_intensity_recommender():
    """
    Function to run the light intensity recommender.

    This function sets up the argument parser, gets the user-defined parameters, and then runs the entire process
    of calculating recommended light intensity, energy savings, and saving the output data to CSV files.

    Parameters:
        None

    Returns:
        None
    """
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", help="List of the municipalities the user wants to preprocess", nargs='+', default=["light price", "moon", "snow", "rain", "cloud", "events"])
    
    args = parser.parse_args()
    params = args.params

    light_intensity_recommender = LightIntensityRecommender()
    light_intensity_recommender.get_input_data()
    light_intensity_recommender.calculate_recommended_light_intensity(params)
    light_intensity_recommender.calculate_intensity_savings()
    light_intensity_recommender.calculate_co2_consumption()
    light_intensity_recommender.save_output_data()

#run_intensity_recommender()

