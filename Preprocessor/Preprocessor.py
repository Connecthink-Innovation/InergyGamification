import os
import shutil

class Preprocessor:
    def __init__(self):
        self.input_data_path = os.path.join("Preprocessor", "input_data")
        self.output_data_path = os.path.join("Preprocessor", "output_data")

    def get_input_data(self):
        current_dir = os.getcwd()

        # Obtener las rutas absolutas de las carpetas "data" en los directorios RSS_Spider y SkyInfo_Spider
        rss_data_path = os.path.abspath(os.path.join(current_dir, "RSS_Spiders", "data"))
        skyinfo_data_path = os.path.abspath(os.path.join(current_dir, "SkyInfo_Spiders", "data"))

        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copiar todos los archivos de las carpetas "data" a la carpeta "input_data"
        self.copy_files_to_input_data(rss_data_path)
        self.copy_files_to_input_data(skyinfo_data_path)

    def copy_files_to_input_data(self, source_dir):
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

preprocessor = Preprocessor()
preprocessor.get_input_data()



