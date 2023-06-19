import os
import shutil

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class Preprocessor:
    def __init__(self):
        self.input_data_path = os.path.join("Preprocessor", "input_data")
        self.temp_data_path = os.path.join("Preprocessor", "temp_data")
        self.output_data_path = os.path.join("Preprocessor", "output_data")

        self.df = pd.DataFrame()

    #GET INPUT DATA
    def get_input_data(self):
        current_dir = os.getcwd()

        # Obtener las rutas absolutas de las carpetas "data" en los directorios RSS_Spider y SkyInfo_Spider
        rss_data_path = os.path.abspath(os.path.join(current_dir, "RSS_Spiders", "data"))
        skyinfo_data_path = os.path.abspath(os.path.join(current_dir, "SkyInfo_Spiders", "data"))
        lightprice_data_path = os.path.abspath(os.path.join(current_dir, "LightPrice_Spiders", "data"))


        # Verificar si la carpeta "input_data" existe, si no, crearla
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copiar todos los archivos de las carpetas "data" a la carpeta "input_data"
        self.copy_files_to_input_data(rss_data_path)
        self.copy_files_to_input_data(skyinfo_data_path)
        self.copy_files_to_input_data(lightprice_data_path)
        
    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

    #PREPROCESS DATA
    def preprocess_data(self):
        self.preprocess_rss_data("rss_canyelles.csv")
        self.preprocess_weather_data("weather.csv")
        self.preprocess_moon_phases("moon_phases.csv")
        self.preprocess_moonrise_moonset("moonrise_moonset.csv")
        self.preprocess_sunrise_sunset("sunrise_sunset.csv")
        self.preprocess_light_prices("light_prices.csv")
        
        self.merge_data()

        self.aggregated_metrics()

    # >> UTILS PREPROCCESS DATA
    def preprocess_rss_data(self, file_name,):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file) 
          

        #Process date components
        df["Date"] = pd.to_datetime(df["Date"])
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Day"] = df["Date"].dt.day

        #Rename components
        df.rename(columns={'Title': 'event_title'}, inplace=True)

        #Delete unrelated variables
        df = df.drop(columns=["Date"], axis=1)

        #Select only act and next day
        """
        PROD. CODE

        current_date = datetime.now().date()
        next_date = current_date + timedelta(days=1)
        """

        #PREPROD. CODE
        current_date = datetime(year=2023, month=6, day=14)
        next_date = datetime(year=2023, month=6, day=15)
        #---------------

        filtered_df = df[
                    (df["Year"] == current_date.year)
                    & (df["Month"] == current_date.month)
                    & (df["Day"] == current_date.day)
                    | (df["Year"] == next_date.year)
                    & (df["Month"] == next_date.month)
                    & (df["Day"] == next_date.day)
                ]
        
        #save
        file_name = "rss_canyelles_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)    


    def preprocess_weather_data(self, file_name, filter_dates=False, date_min=None, date_max=None):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        #Process temperature columns -> F to C
        df['temp_farenheit'] = df['temp_farenheit'].str.split().str[0].astype(float)
        df["temp_celsius"] = df["temp_farenheit"].apply(self.farenheit_to_celsius)

        #Process % columns
        df['precip_percent'] = df['precip_percent'].str.split().str[0].astype(float)
        df['cloud_cover_percent'] = df['cloud_cover_percent'].str.split().str[0].astype(float)
        df['humidity_percent'] = df['humidity_percent'].str.split().str[0].astype(float)

        #Process quantity columns
        df["precip_inches"] = df['precip_inches'].str.split().str[0].astype(float)
        df["precip_mm"] = df["precip_inches"].apply(self.inches_to_mm)

        df["pressure_inches"] = df['pressure_inches'].str.split().str[0].astype(float)
        df["pressure_hPa"] = df['pressure_inches'].apply(self.inches_to_hPa)

        #Process str columns
        df["condition"] = df["condition"].str.lower()

        #Process date components
        df["Date"] = pd.to_datetime(df["Date"])
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Day"] = df["Date"].dt.day

        df["Hour"] = pd.to_datetime(df["Hour"], format="%I:%M %p").dt.time

        #Delete unrelated variables
        df = df.drop(columns=["Unnamed: 0", "temp_farenheit", "feels_like_farenheit", "dew_point_farenheit", "wind_vel", "Date", "precip_inches", "pressure_inches"], axis=1)

        #Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        #save
        file_name = "weather_data_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        df.to_csv(dst_file, index=False)    

    def farenheit_to_celsius(self, degrees_farenheit: float) -> float:

        degrees_celsius = (degrees_farenheit - 32) * (5/9)
        return round(degrees_celsius,2)
    
    def inches_to_mm(self, inches:float) -> float:
        mm = inches * 25.4
        return mm
    
    def inches_to_hPa(self, inches:float) -> float:
        hPa = inches * 33.863889532610884
        return round(hPa,2)


    def preprocess_moon_phases(self, file_name):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        converted_data = []
        for i, row in df.iterrows():
            year = row['Year']
            dates = row[['New Moon', 'First Quarter', 'Full Moon', 'Third Quarter']]

            phases = ["New Moon", "First Quarter", "Full Moon", "Third Quarter"]
            phases_dates_list = []

            if not pd.isna(dates['New Moon']):
                new_moon_date = datetime.strptime(dates['New Moon'], '%d %b')
                new_moon_date = new_moon_date.replace(year=year)
                phases_dates_list.append(new_moon_date)
                new_moon_date_day = new_moon_date.day
            else:
                new_moon_date_day = -1

            if not pd.isna(dates['First Quarter']):
                first_quarter_date = datetime.strptime(dates['First Quarter'], '%d %b')
                first_quarter_date = first_quarter_date.replace(year=year)
                phases_dates_list.append(first_quarter_date)
                first_quarter_date_day = first_quarter_date.day
            else:
                first_quarter_date_day = -1

            if not pd.isna(dates['Full Moon']):
                full_moon_date = datetime.strptime(dates['Full Moon'], '%d %b')
                full_moon_date = full_moon_date.replace(year=year)
                phases_dates_list.append(full_moon_date)
                full_moon_date_day = full_moon_date.day
            else:
                full_moon_date_day = -1

            if not pd.isna(dates['Third Quarter']):
                third_quarter_date = datetime.strptime(dates['Third Quarter'], '%d %b')
                third_quarter_date = third_quarter_date.replace(year=year)
                phases_dates_list.append(third_quarter_date)
                third_quarter_date_day = third_quarter_date.day
            else:
                third_quarter_date_day = -1
                

            start_date = phases_dates_list[0]
            end_date = phases_dates_list[-1]
            days_to_fill = (end_date-start_date).days

            month = start_date.month
            for j in range(days_to_fill+1):
                act_date = start_date + timedelta(days=j)
                day = act_date.day
                month = act_date.month
                year = act_date.year

                if day == new_moon_date_day:
                    phase = 'New Moon'
                elif day == first_quarter_date_day:
                    phase = 'First Quarter'
                elif day == full_moon_date_day:
                    phase = 'Full Moon'
                elif day == third_quarter_date_day:
                    phase = 'Third Quarter'
                else:
                    phase = last_phase

                if phase is not None:
                    converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    last_phase = phase

            # Añadir días faltantes entre las fechas
            if i < len(df) - 1:
                next_dates = df.loc[i + 1, ['New Moon', 'First Quarter', 'Full Moon', 'Third Quarter']].values
                next_dates_year = df.loc[i+1, 'Year']

                if pd.notnull(next_dates[0]):
                    end_date = datetime.strptime(next_dates[0], '%d %b')
                    end_date = end_date.replace(year=next_dates_year)
                    missing_days = (end_date - act_date).days
                    for k in range(1, missing_days):
                        new_date = act_date + timedelta(days=k)
                        year = new_date.year
                        month = new_date.month
                        day = new_date.day
                        converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    continue
                elif pd.notnull(next_dates[1]):
                    end_date = datetime.strptime(next_dates[1], '%d %b')
                    end_date = end_date.replace(year=next_dates_year)
                    missing_days = (end_date - act_date).days
                    for k in range(1, missing_days):
                        new_date = act_date + timedelta(days=k)
                        year = new_date.year
                        month = new_date.month
                        day = new_date.day
                        converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    continue

                elif pd.notnull(next_dates[2]):
                    end_date = datetime.strptime(next_dates[2], '%d %b')
                    end_date = end_date.replace(year=next_dates_year)
                    missing_days = (end_date - act_date).days
                    for k in range(1, missing_days):
                        new_date = act_date + timedelta(days=k)
                        year = new_date.year
                        month = new_date.month
                        day = new_date.day
                        converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    continue

                else:
                    end_date = datetime.strptime(next_dates[3], '%d %b')
                    end_date = end_date.replace(year=next_dates_year)
                    missing_days = (end_date - act_date).days
                    for k in range(1, missing_days):
                        new_date = act_date + timedelta(days=k)
                        month = new_date.month
                        day = new_date.day
                        converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    continue

        #Save in dataframe
        converted_df = pd.DataFrame(converted_data)
        #Sort by date
        converted_df = converted_df.sort_values(by=["Year", "Month", "Day"])
        
        #Select only act and next day
        """
        PROD. CODE

        current_date = datetime.now().date()
        next_date = current_date + timedelta(days=1)
        """

        #PREPROD. CODE
        current_date = datetime(year=2023, month=6, day=14)
        next_date = datetime(year=2023, month=6, day=15)
        #---------------

        filtered_df = converted_df[
            (converted_df["Year"] == current_date.year)
            & (converted_df["Month"] == current_date.month)
            & (converted_df["Day"] == current_date.day)
            | (converted_df["Year"] == next_date.year)
            & (converted_df["Month"] == next_date.month)
            & (converted_df["Day"] == next_date.day)
        ]

        #save
        file_name = "moon_phases_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def preprocess_moonrise_moonset(self, file_name):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        
        #Convert NaN cells to standard format
        df = df.replace("-", pd.NaT) # "-" to NaN

        #Aggregated columns 1
        df["Moonrise"] = df["Moonrise_left"].fillna(df["Moonrise_right"]) #Get the valid Moonrise
                
        #Drop meridian days
        df = df.dropna(subset=['Moonset', 'Moonrise'])


        #Aggregated columns 2
        df["moon_hours"] = round(abs((pd.to_datetime(df["Moonrise"], format="%H:%M") - pd.to_datetime(df["Moonset"], format="%H:%M")).dt.total_seconds() / 3600), 2) #Calculate moon duration
        df["moon_illumination_percent"] = df["Illumination"].str.rstrip("%").astype(float)
        df["moon_distance"] = df["Distance (km)"].str.replace(",", ".").astype(float)

        #Process date components   
        df["Moonset"] = pd.to_datetime(df["Moonset"], format="%H:%M").dt.time
        df["Moonrise"] = pd.to_datetime(df["Moonrise"], format="%H:%M").dt.time

        #Delete useless columns 
        df = df.drop(['Moonrise_left', 'Moonrise_right', "Time", "Illumination", "Distance (km)"], axis=1)

        #Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        #Select only act and next day
        """
        PROD. CODE

        current_date = datetime.now().date()
        next_date = current_date + timedelta(days=1)
        """

        #PREPROD. CODE
        current_date = datetime(year=2023, month=6, day=14)
        next_date = datetime(year=2023, month=6, day=15)
        #---------------
        
        filtered_df = df[
            (df["Year"] == current_date.year)
            & (df["Month"] == current_date.month)
            & (df["Day"] == current_date.day)
            | (df["Year"] == next_date.year)
            & (df["Month"] == next_date.month)
            & (df["Day"] == next_date.day)
        ]

        #save
        file_name = "moonrise_moonset_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def preprocess_sunrise_sunset(self, file_name):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        #Aggregated columns
        df["sun_hours"] = df["Length"].apply(self.datetime_to_hours) #Calculate sun hours (hours format)
        df["civil_sun_hours"] = (pd.to_datetime(df['End_civil_twilight'], format='%H:%M') - pd.to_datetime(df['Start_civil_twilight'], format='%H:%M')).apply(lambda x: round(x.total_seconds() / 3600, 4)) #Calculate civil sun hours
        df["sun_hours_diff_in_minutes"] = round((pd.to_numeric(df["Diff."].str.extract('(\d+):')[0]) + pd.to_numeric(df["Diff."].str.extract(':(\d+)')[0]) / 60), 2) # Calculate sun hours diff.

        #Process date components   
        df["Sunset"] = pd.to_datetime(df["Sunset"], format="%H:%M").dt.time
        df["Sunrise"] = pd.to_datetime(df["Sunrise"], format="%H:%M").dt.time


        #Rename and drop columns
        df = df.drop(columns=["Length", "Diff."])

        #Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        #Select only act and next day
        """
        PROD. CODE

        current_date = datetime.now().date()
        next_date = current_date + timedelta(days=1)
        """

        #PREPROD. CODE
        current_date = datetime(year=2023, month=6, day=14)
        next_date = datetime(year=2023, month=6, day=15)
        #---------------
        
        filtered_df = df[
            (df["Year"] == current_date.year)
            & (df["Month"] == current_date.month)
            & (df["Day"] == current_date.day)
            | (df["Year"] == next_date.year)
            & (df["Month"] == next_date.month)
            & (df["Day"] == next_date.day)
        ]

        #save
        file_name = "sunrise_sunset_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def datetime_to_hours(self, datatime, format="%H:%M:%S"): #Convert datatime to hours 
        time = datetime.strptime(datatime, format)
        total_hours = time.hour + time.minute / 60 + time.second / 3600
        return round(total_hours, 4)
    

    def preprocess_light_prices(self, file_name):
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file, encoding="latin1")

        #Process date components
        df["Date"] = pd.to_datetime(df["Date"])
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Day"] = df["Date"].dt.day
        
        df["Hour"] = df['hour_range'].str.split().str[0]
        df["Hour"] = pd.to_datetime(df["Hour"], format="%H:%M").dt.time

        #Process light price component
        df["light_price_kwh"] = df['light_price_kwh'].str.split().str[0].astype(float)

        #Delete unrelated variables
        df = df.drop(columns=["hour_range"], axis=1)

        #Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        #save
        file_name = "light_prices_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        df.to_csv(dst_file, index=False)    
        


    def merge_data(self):

        for file_name in os.listdir(self.temp_data_path):
            if file_name.endswith(".csv"):

                file_path = os.path.join(self.temp_data_path, file_name)
                print(file_path)
                other_df = pd.read_csv(file_path)

                if self.df.empty:
                    self.df = other_df
                else:
                    if "Hour" in self.df.columns and "Hour" in other_df.columns:
                        self.df = self.df.merge(other_df, on=["Year", "Month", "Day", "Hour"], how="outer")
                    else:
                        self.df = self.df.merge(other_df, on=["Year", "Month", "Day"], how="outer")
    
    

    def aggregated_metrics(self,):

        upper_light_price_mean = []
        is_night = []
        is_day = []
        needs_artif_light = []
        moon_phase_mult = []

        for index, row in self.df.iterrows():
            if row["light_price_kwh"] > self.df["light_price_kwh"].mean():
                upper_light_price_mean.append(True)
            else:
                upper_light_price_mean.append(False)

            if row["Moonset"] < row["Hour"] < row["Moonrise"]:
                is_night.append(True)
            else:
                is_night.append(False)

            if row["Sunset"] < row["Hour"] < row["Sunrise"]:
                is_day.append(True)
            else:
                is_day.append(False)
            
            if row["Start_civil_twilight"] < row["Hour"] < row["End_civil_twilight"]:
                needs_artif_light.append(False)
            else:
                needs_artif_light.append(True)

            if row["moon_phase"] == "New Moon":
                moon_phase_mult.append(1)
            elif row["moon_phase"] == "First Quarter" or row["moon_phase"] == "Third Quarter":
                moon_phase_mult.append(2)
            else: 
                moon_phase_mult.append(3)

        self.df["upper_light_price_mean"] = upper_light_price_mean
        self.df["is_night"] = is_night
        self.df["is_day"] = is_day
        self.df["needs_artif_light"] = needs_artif_light 
        self.df["moon_phase_mult"] = moon_phase_mult
        

            

    #SAVE OUTPUT DATA
    def save_output_data(self):

        file_name = "processed_data.csv"
        dst_file = os.path.join(self.output_data_path, file_name)
        self.df.to_csv(dst_file, index=False)



#DEBUG MAIN
preprocessor = Preprocessor()
preprocessor.get_input_data()
preprocessor.preprocess_data()
preprocessor.save_output_data()



