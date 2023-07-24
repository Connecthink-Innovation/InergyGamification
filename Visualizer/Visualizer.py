import seaborn as sns
import matplotlib.pyplot as plt

import pandas as pd
import os
import shutil
from datetime import datetime, timedelta

class Visualizer:

    def __init__(self):

        self.input_data_path = os.path.join("Visualizer", "input_data")
        self.output_data_path = os.path.join("Visualizer", "output_data")

        self.real_intensity_vs_recommended_plots = []

    # GET INPUT DATA
    def get_input_data(self):
        current_dir = os.getcwd()

        # Obtener las rutas absolutas de las carpetas "output_data" en el directorio Light_intensity_recommender
        light_intensity_recommender_data_path = os.path.abspath(os.path.join(current_dir, "Light_intensity_recommender", "output_data"))

        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copiar todos los archivos de las carpetas "output_data" del Light_intensity_recommender a la carpeta "input_data" del Visualizer
        self.copy_files_to_input_data(light_intensity_recommender_data_path)

    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

    def visualize_real_intensity_vs_recommended(self,):
        
        for file_name in os.listdir(self.input_data_path):
            if file_name.endswith(".csv"):
                
                zone = (os.path.splitext(file_name)[0]).split("_intensity_")[1]
                file_path = os.path.join(self.input_data_path, file_name)
                df = pd.read_csv(file_path)

                # Obtener la fecha de hoy y la de mañana
                today = datetime.now().strftime('%Y-%m-%d')
                tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

                # Crear la gráfica de barras
                sns.set(style='whitegrid')
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.barplot(data=df, x='Hour', y='real_intensity', color='red', label='Real Intensity')
                sns.barplot(data=df, x='Hour', y='recommended_intensity', color='green', label='Recommended Intensity')
                plt.xlabel('Hour')
                plt.ylabel('Intensity')
                plt.title('Real Intensity vs. Recommended Intensity')
                # Rotar los nombres de las horas en el eje x
                plt.xticks(rotation=45)
                # Agregar la fecha de hoy y la de mañana en la leyenda
                plt.legend(title=f'Date: {today}-{tomorrow}\nZone: {zone}')
                
                # Guardar el plot y su zona en la lista
                self.real_intensity_vs_recommended_plots.append((fig, zone))
                #plt.show()

    #SAVE OUTPUT DATA
    def save_output_data(self):

        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.output_data_path):
            os.makedirs(self.output_data_path)

        #Iter real intensity vs recommended plots
        for real_intensity_vs_recommended_plot in self.real_intensity_vs_recommended_plots:

            fig = real_intensity_vs_recommended_plot[0]
            zone = real_intensity_vs_recommended_plot[1]

            # Nombre del archivo para guardar el plot
            file_name = f'real_intensity_vs_recommended_plot_{zone}.png'
            dst_file = os.path.join(self.output_data_path, file_name)
            
            # Guardar el plot en formato de imagen
            fig.savefig(dst_file, dpi=300, bbox_inches='tight')


#test

def run_visualizer():

    visualizer = Visualizer()
    visualizer.get_input_data()
    visualizer.visualize_real_intensity_vs_recommended()
    visualizer.save_output_data()

run_visualizer()

