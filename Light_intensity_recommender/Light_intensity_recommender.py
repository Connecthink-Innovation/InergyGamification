import os
import shutil

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


    #CALCULATE RECOMMENDED LIGHT INTENSITY
    def calculate_recommended_light_intensity(self):
        csv_file = os.path.join(self.input_data_path, "processed_data.csv")
        df = pd.read_csv(csv_file)

        df['Recommended_light_intensity'] = df.apply(self.light_intensity_formula, axis=1)

        # Guardar
        file_name = "recommended_light_intensity.csv"
        dst_file = os.path.join(self.output_data_path, file_name)
        df.to_csv(dst_file, index=False)

    # >> UTILS GIP
    def light_intensity_formula(self, row):
        # Define coefficients
        A = -0.3
        B = 0.2
        C = -0.4
        D = 0.1
        return 100 + (A * row['moon_illumination_percent'] + B * row['moon_hours'] + C * row['sun_hours'] + D * row['sun_hours_diff_in_minutes'])


    #CALCULATE ENERGY SAVING
    def calculate_energy_saving(self,):
        pass



    


light_itensity_recommender = LightIntensityRecommender()
light_itensity_recommender.get_input_data()
light_itensity_recommender.calculate_recommended_light_intensity()