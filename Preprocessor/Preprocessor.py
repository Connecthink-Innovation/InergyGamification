import os
import shutil
import copy

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import random
from scipy.spatial import cKDTree

from deep_translator import GoogleTranslator
from langdetect import detect
import pycountry


class Preprocessor:
    def __init__(self, mode, events_source):
        #Mode debug or prod
        self.mode = mode

        # Indicates the source of the events to be used
        self.events_source = events_source

        # Initialize paths for input, temporary, and output data directories
        self.input_data_path = os.path.join("Preprocessor", "input_data")
        self.temp_data_path = os.path.join("Preprocessor", "temp_data")
        self.output_data_path = os.path.join("Preprocessor", "output_data")

        # Initialize dataframes to store data
        self.df_next = pd.DataFrame() # Dataframe to store the common future data of all zones (sky data, weather, etc.)
        self.df_next_list = [] # List to store each future data of each zone 

        self.df_previous = pd.DataFrame() # Dataframe to store the common previous data of all zones

        # Initialize a list to store zones (not shown how it is used here)
        self.zones = []


    #GET INPUT DATA
    def get_input_data(self,):
        """
        Method to collect input data from multiple directories and 
        copy CSV files to the "input_data" folder.

        Parameters:
            None

        Returns:
            None
        """

        # Get the current working directory
        current_dir = os.getcwd()

        # Obtain the absolute paths of the "data" folders in specific directories
        nodeclassifier_data_path = os.path.abspath(os.path.join(current_dir, "Node_classifier", "output_data"))
        skyinfo_data_path = os.path.abspath(os.path.join(current_dir, "SkyInfo_Spiders", "data"))
        lightprice_data_path = os.path.abspath(os.path.join(current_dir, "LightPrice_Spiders", "data"))
        rss_data_path = os.path.abspath(os.path.join(current_dir, "RSS_Spiders", "data"))
        events_data_path = os.path.abspath(os.path.join(current_dir, "Event_generator", "data"))

        # Check if the "input_data" folder exists, if not, create it
        if not os.path.exists(self.input_data_path):
            os.makedirs(self.input_data_path)

        # Copy all the CSV files from the "data" folders to the "input_data" folder using "copy_files_to_input_data" method
        self.copy_files_to_input_data(nodeclassifier_data_path)
        self.copy_files_to_input_data(skyinfo_data_path)
        self.copy_files_to_input_data(lightprice_data_path)
        self.copy_files_to_input_data(rss_data_path)
        self.copy_files_to_input_data(events_data_path)
        
    # >> UTILS GIP
    def copy_files_to_input_data(self, source_dir):
        """
        Method to copy CSV files from the source directory to the "input_data" folder.

        Parameters:
            source_dir (str): The absolute path of the source directory containing CSV files.

        Returns:
            None  
        """

        # Iterate over the files in the source directory
        for file_name in os.listdir(source_dir):
            # Check if the file is a CSV file
            if file_name.endswith(".csv"):
                src_file = os.path.join(source_dir, file_name)
                dst_file = os.path.join(self.input_data_path, file_name)
                # Check if the file is a regular file before copying it
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)

    #PREPROCESS DATA
    def preprocess_data(self):
        """
        Method to preprocess the input data for the Preprocessor.

        Parameters:
            None

        Returns:
            None
        """

        # Preprocess the node classifier data
        self.preprocess_node_classifier_data("classified_nodes.csv")
        
        # Preprocess the RSS data (commented out)
        #self.preprocess_rss_data("rss_canyelles.csv")
        
        # Preprocess the Google events data for each zone
        if self.events_source == "google":
            events_filename = "google_events.csv"
        else:
            events_filename = "fake_events.csv"

        for zone in self.zones:
            self.preprocess_events_data(events_filename, zone=zone, sampling=random.choice(range(1,5)))

        # Preprocess weather data for the previous day
        self.preprocess_weather_previous_data("weather_previous.csv")

        # Preprocess weather data for the next day 
        self.preprocess_weather_next_data("weather_next.csv")
        
        # Preprocess moon phases data 
        self.preprocess_moon_phases("moon_phases.csv")
        
        # Preprocess moonrise and moonset data    
        self.preprocess_moonrise_moonset("moonrise_moonset.csv")
        
        # Preprocess sunrise and sunset data
        self.preprocess_sunrise_sunset("sunrise_sunset.csv")
        
        # Preprocess light prices data
        self.preprocess_light_prices("light_prices.csv")
      
        # Merge sky-related data (next day's weather, moon phases, sunrise, sunset, etc.)
        self.merge_sky_data()
        
        # Aggregate sky-related metrics
        self.aggregated_sky_metrics()

        # Merge events data for each zone
        self.merge_events_data()

    # >> UTILS PREPROCCESS DATA
    def preprocess_node_classifier_data(self, file_name,):
        """
        Method to preprocess the node classifier data and 
        save the processed data as a new CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing the node classifier data.

        Returns:
            None
        """

        # Read data from the node classifier CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        # Get unique zones
        self.zones = list(df.zone.unique())
        
        # Define aggregation functions for each column
        agg_functions = {
            'id': lambda x: list(x),
            'type': lambda x: list(x),
            'ebox_id': lambda x: list(x),
            'coordinates': lambda x: list(x)   
        }
        
        # Group data by 'zone' and apply aggregation functions
        grouped_df = df.groupby('zone').agg(agg_functions).reset_index()

        # Rename the aggregated columns
        grouped_df.rename(columns={
            'id': 'id_list',
            'type': 'type_list',
            'ebox_id': 'ebox_id_list',
            'coordinates': 'coordinates_list'
        }, inplace=True)

        # Save the processed data to a new CSV file
        file_name = "classified_nodes_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        grouped_df.to_csv(dst_file, index=False)          


    def  preprocess_events_data(self, file_name, zone, sampling=None):
        """
        Preprocesses Google events data from a CSV file.
        
        Parameters:
            file_name (str): The name of the CSV file containing event data.
            zone (str): The zone for which events need to be processed.
            sampling (int, optional): The number of events to sample. If None, all events in the specified zone are considered.

        Returns:
            None
        """

        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        # Find the zone of the illumination closest to each event's location
        csv_file = os.path.join(self.input_data_path, "classified_nodes.csv")
        df_nodes_zone = pd.read_csv(csv_file)

        # Use cKDTree to find nearest zones
        tree = cKDTree(df_nodes_zone[['lat', 'lon']])
        _, indices = tree.query(df[['lat', 'lon']])
        df['zone'] = df_nodes_zone.loc[indices, 'zone'].values

        # Filter data for the current zone
        df = df[df["zone"] == zone]

        # If a sampling has been indicated, we reduce the number of events to the indicated sampling
        if sampling:
            if sampling > 0 and len(df) > 0:
                if sampling < len(df):
                    df = df.sample(n=sampling)
                    df = df.reset_index(drop=True)
                else:
                    df = df.copy()

            else:
                df = pd.DataFrame(columns=df.columns)

        #Translate to english

        # > Detect the original events language
        if len(df) > 0:
            first_description = df.loc[0, 'Title']
            detected_lang_code  = detect(first_description)
            detected_lang_name = pycountry.languages.get(alpha_2=detected_lang_code).name

            # > Translate only if it is not in English
            if detected_lang_code != 'en':
                print(f"Translating events from {detected_lang_name} to English...")

                # > Initialize the translator
                translator = GoogleTranslator(source='auto', target='en')

                # > Apply translation with retries to each column
                df['Title'] = df['Title'].apply(lambda x: translator.translate(x))
                df['Location'] = df['Location'].apply(lambda x: translator.translate(x))
                df['Description'] = df['Description'].apply(lambda x: translator.translate(x))

        #PROD. CODE
        if self.mode == "prod":
            # Get the current date and time
            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day
        

        #DEBUG. CODE
        if self.mode == "debug":
            current_date = datetime(year=2023, month=6, day=26)
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day
        #---------------

        new_data = []
        # Iterate over the rows of the DataFrame
        for index, row in df.iterrows():

            hour_range = row['Schedule']

            start_time, end_time = hour_range.split('-')
                    
            # Convert the times into datetime objects
            start_time = datetime.strptime(((datetime.strptime(start_time, '%H:%M').time()).strftime('%H:%M:%S')), '%H:%M:%S')
            end_time = datetime.strptime(((datetime.strptime(end_time, '%H:%M').time()).strftime('%H:%M:%S')), '%H:%M:%S' )

            # Truncate date_hours
            start_time_truncated = self.truncate_hour(start_time)
            end_time_truncated = self.truncate_hour(end_time)

            # Calculate the time difference between the start time and end time
            time_diff = end_time - start_time

            # Extract common columns
            title = row["Title"]
            location = row["Location"]
            description = row["Description"]
            zone = row["zone"]
            lat = row["lat"]
            lon = row["lon"]

            # Iterate over the range of hours to create 
            # as many rows as there are hours between the original time range
            num_hours = int(time_diff.seconds // 3600)+1
            change_day=False
            for hour in range(num_hours):

                new_row = []
                
                act_hour = start_time_truncated + timedelta(hours=hour)
                act_hour = pd.Timestamp(act_hour)
                act_hour = act_hour.time()

                if act_hour == (pd.Timestamp('00:00:00')).time():
                    change_day=True

                if change_day:
                    act_date = current_date + timedelta(days=1)
                    act_year = act_date.year
                    act_month = act_date.month
                    act_day = act_date.day
                else:
                    act_year = current_year
                    act_month = current_month
                    act_day = current_day


                new_row = [title, location, description, zone, lat, lon, act_year, act_month, act_day, act_hour]
                new_data.append(new_row)

        # Create new df
        processed_df = pd.DataFrame(new_data, columns=["event_title", "event_location", "event_description", "event_zone", "event_lat", "event_lon", "Year", "Month", "Day", "Hour"])
        processed_df["Day"] = processed_df["Day"].replace({14: 26, 15: 27})
        processed_df["Month"] = processed_df["Month"].replace({7: 6})

        # Group by Year, Month, Day, and Hour, and create lists of the corresponding values
        if len(processed_df) > 0:
            grouped_df = processed_df.groupby(["Year", "Month", "Day", "Hour"]).agg({
                "event_location": lambda x: x.tolist(),
                "event_title": lambda x: x.tolist(),
                "event_description": lambda x: x.tolist(),
                "event_zone": lambda x: x.tolist(),
                "event_lat": lambda x: x.tolist(),
                "event_lon": lambda x: x.tolist()
            }).reset_index()
        else:
            grouped_df = processed_df

        # Rename the created columns
        grouped_df.rename(columns={
            "event_location": "events_locations",
            "event_title": "events_titles",
            "event_description": "events_descriptions",
            "event_zone": "events_zones",
            "event_lat": "events_lats",
            "event_lon": "events_lons"
        }, inplace=True)       



        # Save the processed data to a CSV file
        file_name = f'google_events_processed_{zone}.csv'
        dst_file = os.path.join(self.temp_data_path, file_name)
        grouped_df.to_csv(dst_file, index=False)    

    def truncate_hour(self, date_hour):
        """
        Truncates the given datetime object to the nearest hour.

        Parameters:
            date_hour (datetime): The datetime object to truncate.

        Returns:
            datetime: The truncated datetime object with minutes set to 0.
        """
            
        # Extract the hour and minutes
        hour = date_hour.hour
        minutes = date_hour.minute

        # Truncate to the nearest hour
        minutes = 0

        # Create the new datetime object
        date_hour_truncated = date_hour.replace(minute=minutes)

        return date_hour_truncated

    def preprocess_rss_data(self, file_name,):
        """
        Preprocesses RSS data from a CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing RSS data.

        Returns:
            None
        """

        # Read data from the CSV file    
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file) 
          

        # Process date components
        df["Date"] = pd.to_datetime(df["Date"])
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Day"] = df["Date"].dt.day

        # Rename components
        df.rename(columns={'Title': 'event_title'}, inplace=True)

        #Delete unrelated variables
        df = df.drop(columns=["Date"], axis=1)

        #Select only act and next day

        #PROD. CODE
        if self.mode == "prod":
            current_date = datetime.now().date()
            next_date = current_date + timedelta(days=1)
        

        #DEBUG. CODE
        if self.mode == "debug":
            current_date = datetime(year=2023, month=6, day=26)
            next_date = datetime(year=2023, month=6, day=27)
        #---------------

        # Filter RSS data to current and next day
        filtered_df = df[
                    (df["Year"] == current_date.year)
                    & (df["Month"] == current_date.month)
                    & (df["Day"] == current_date.day)
                    | (df["Year"] == next_date.year)
                    & (df["Month"] == next_date.month)
                    & (df["Day"] == next_date.day)
                ]
        
        # Save the processed data to a new CSV file
        file_name = "rss_canyelles_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)    

    def preprocess_weather_previous_data(self, file_name, filter_dates=False, date_min=None, date_max=None):
        """
        Method to preprocess the weather data for the previous day 
        and save the processed data as a new CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing the weather data for the previous day.
            filter_dates (bool): A flag indicating whether to filter the data based on date_min and date_max.
            date_min (str): The minimum date in the format "YYYY-MM-DD" for filtering the data (optional).
            date_max (str): The maximum date in the format "YYYY-MM-DD" for filtering the data (optional).

        Returns:
            None        
        """
        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        # Process temperature columns -> F to C
        df['temp_farenheit'] = df['temp_farenheit'].str.split().str[0].astype(float)
        df["temp_celsius"] = df["temp_farenheit"].apply(self.farenheit_to_celsius)

        #Process % columns
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
        df = df.drop(columns=["Unnamed: 0", "temp_farenheit", "dew_point_farenheit", "wind", "wind_vel", "wind_gust", "Date", "precip_inches", "pressure_inches"], axis=1)

        #Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        # Save processed data to a new CSV file named "weather_previous_data_processed.csv"
        file_name = "weather_previous_data_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        df.to_csv(dst_file, index=False)    


    def preprocess_weather_next_data(self, file_name, filter_dates=False, date_min=None, date_max=None):
        """
        Method to preprocess the weather data for the next day 
        and save the processed data as a new CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing the weather data for the next day.
            filter_dates (bool): A flag indicating whether to filter the data based on date_min and date_max.
            date_min (str): The minimum date in the format "YYYY-MM-DD" for filtering the data (optional).
            date_max (str): The maximum date in the format "YYYY-MM-DD" for filtering the data (optional).

        Returns:
            None        
    
        """
    
        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        if self.mode == "prod":
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
        
        else:
            # Process temperature columns -> F to C
            df['temp_farenheit'] = df['temp_farenheit'].str.split().str[0].astype(float)
            df["temp_celsius"] = df["temp_farenheit"].apply(self.farenheit_to_celsius)

            #Process % columns
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
            df = df.drop(columns=["Unnamed: 0", "temp_farenheit", "dew_point_farenheit", "wind", "wind_vel", "wind_gust", "Date", "precip_inches", "pressure_inches"], axis=1)

            #Sort values by date
            df = df.sort_values(by=["Year", "Month", "Day"])

        # Save processed data to a new CSV file named "weather_previous_data_processed.csv"
        file_name = "weather_next_data_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        df.to_csv(dst_file, index=False)  

    def farenheit_to_celsius(self, degrees_farenheit: float) -> float:
        """
        Convert a temperature value from Fahrenheit to Celsius.

        Parameters:
            degrees_fahrenheit (float): The temperature value in Fahrenheit.

        Returns:
            float: The temperature value converted to Celsius.       
        """

        # Convert Fahrenheit to Celsius
        degrees_celsius = (degrees_farenheit - 32) * (5/9)
        return round(degrees_celsius,2)
    
    def inches_to_mm(self, inches:float) -> float:
        """
        Convert a length value from inches to millimeters.

        Parameters:
            inches (float): The length value in inches.

        Returns:
            float: The length value converted to millimeters.
        """

        # Convert inches to millimeters
        mm = inches * 25.4
        return mm
    
    def inches_to_hPa(self, inches:float) -> float:
        """
        Convert a pressure value from inches of mercury to hectopascals (hPa).

        Parameters:
            inches (float): The pressure value in inches of mercury.

        Returns:
            float: The pressure value converted to hectopascals (hPa).       
        """
        
        # Convert inches of mercury to hectopascals (hPa)
        hPa = inches * 33.863889532610884
        return round(hPa,2)


    def preprocess_moon_phases(self, file_name):
        """
        Preprocess moon phase data from the input CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing moon phase data.

        Returns:
            None
        """

        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        # Initialize a list to store the converted moon phase data  
        converted_data = []

        # Iterate over the rows in the DataFrame
        for i, row in df.iterrows():
            year = row['Year']
            dates = row[['New Moon', 'First Quarter', 'Full Moon', 'Third Quarter']]

            # List of possible moon phases in order
            phases = ["New Moon", "First Quarter", "Full Moon", "Third Quarter"]
            phases_dates_list = []

            # Process each moon phase date and add it to the phases_dates_list if available
            if not pd.isna(dates['New Moon']):
                new_moon_date = datetime.strptime(dates['New Moon'], '%d %b')
                new_moon_date = new_moon_date.replace(year=year)
                phases_dates_list.append(new_moon_date)
                new_moon_date_day = new_moon_date.day
            else:
                new_moon_date_day = -1
            
            # Similar processing for other moon phases
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
                
            # Calculate the number of days to fill between the start and end date
            start_date = phases_dates_list[0]
            end_date = phases_dates_list[-1]
            days_to_fill = (end_date-start_date).days

            month = start_date.month
            # Iterate over the days between start and end date to add the corresponding moon phase
            for j in range(days_to_fill+1):
                act_date = start_date + timedelta(days=j)
                day = act_date.day
                month = act_date.month
                year = act_date.year

                # Determine the moon phase for the current day based on the available phase dates
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

                # Append the converted data for the current day and phase
                if phase is not None:
                    converted_data.append({'Year': year, 'Month': month, 'Day': day, 'moon_phase': phase})
                    last_phase = phase

            # Add missing days between the dates of consecutive rows in the input DataFrame
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

        # Save the converted moon phase data to a new DataFrame
        converted_df = pd.DataFrame(converted_data)
        
        # Sort the converted DataFrame by date
        converted_df = converted_df.sort_values(by=["Year", "Month", "Day"])
        
        # Select only the moon phases for the current and next day (specified in PROD/PREPROD CODE)

        #PROD. CODE
        if self.mode == "prod":
            current_date = datetime.now().date()
            next_date = current_date + timedelta(days=1)


        #DEBUG. CODE
        if self.mode == "debug":
            current_date = datetime(year=2023, month=6, day=26)
            next_date = datetime(year=2023, month=6, day=27)
        #---------------

        # Filter moon phase data to current and next day
        filtered_df = converted_df[
            (converted_df["Year"] == current_date.year)
            & (converted_df["Month"] == current_date.month)
            & (converted_df["Day"] == current_date.day)
            | (converted_df["Year"] == next_date.year)
            & (converted_df["Month"] == next_date.month)
            & (converted_df["Day"] == next_date.day)
        ]

        # Save the filtered moon phase data to a new CSV file named "moon_phases_processed.csv"
        file_name = "moon_phases_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def preprocess_moonrise_moonset(self, file_name):
        """
        Preprocess moonrise and moonset data from the input CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing moonrise and moonset data.

        Returns:
            None       
        """

        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        
        # Convert NaN cells to standard format
        df = df.replace("-", pd.NaT) # "-" to NaN

        # Aggregate Moonrise data from two columns into one
        df["Moonrise"] = df["Moonrise_left"].fillna(df["Moonrise_right"]) #Get the valid Moonrise
                
        # Drop rows with missing Moonset or Moonrise data (meridian days)
        df = df.dropna(subset=['Moonset', 'Moonrise'])

        # Calculate additional moon-related columns
        df["moon_hours"] = round(abs((pd.to_datetime(df["Moonrise"], format="%H:%M") - pd.to_datetime(df["Moonset"], format="%H:%M")).dt.total_seconds() / 3600), 2) #Calculate moon duration
        df["moon_illumination_percent"] = df["Illumination"].str.rstrip("%").astype(float)
        df["moon_distance"] = df["Distance (km)"].str.replace(",", ".").astype(float)

        # Process date components of Moonset and Moonrise times
        df["Moonset"] = pd.to_datetime(df["Moonset"], format="%H:%M").dt.time
        df["Moonrise"] = pd.to_datetime(df["Moonrise"], format="%H:%M").dt.time

        # Delete columns that are no longer needed
        df = df.drop(['Moonrise_left', 'Moonrise_right', "Time", "Illumination", "Distance (km)"], axis=1)

        # Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        # Select only the moonrise and moonset data for the current and next day (specified in PROD/PREPROD CODE)
    
        #PROD. CODE
        if self.mode == "prod":
            current_date = datetime.now().date()
            next_date = current_date + timedelta(days=1)
 

        #DEBUG. CODE
        if self.mode == "debug":
            current_date = datetime(year=2023, month=6, day=26)
            next_date = datetime(year=2023, month=6, day=27)
        #---------------
        
        # Filter moon data to current and next day
        filtered_df = df[
            (df["Year"] == current_date.year)
            & (df["Month"] == current_date.month)
            & (df["Day"] == current_date.day)
            | (df["Year"] == next_date.year)
            & (df["Month"] == next_date.month)
            & (df["Day"] == next_date.day)
        ]

        # Save the filtered moonrise and moonset data to a new CSV file named "moonrise_moonset_processed.csv"
        file_name = "moonrise_moonset_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def preprocess_sunrise_sunset(self, file_name):
        """
        Preprocess sunrise and sunset data from the input CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing sunrise and sunset data.

        Returns:
            None        
        """

        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file)

        # Calculate additional columns for sunrise and sunset data
        df["sun_hours"] = df["Length"].apply(self.datetime_to_hours) #Calculate sun hours (hours format)
        df["civil_sun_hours"] = (pd.to_datetime(df['End_civil_twilight'], format='%H:%M') - pd.to_datetime(df['Start_civil_twilight'], format='%H:%M')).apply(lambda x: round(x.total_seconds() / 3600, 4)) #Calculate civil sun hours
        df["sun_hours_diff_in_minutes"] = round((pd.to_numeric(df["Diff."].str.extract('(\d+):')[0]) + pd.to_numeric(df["Diff."].str.extract(':(\d+)')[0]) / 60), 2) # Calculate sun hours diff.

        # Process date components of Sunrise and Sunset times
        df["Sunset"] = pd.to_datetime(df["Sunset"], format="%H:%M").dt.time
        df["Sunrise"] = pd.to_datetime(df["Sunrise"], format="%H:%M").dt.time

        # Drop columns that are no longer needed
        df = df.drop(columns=["Length", "Diff.", "Start_astronomical_twilight", "End_astronomical_twilight", 
                              "Start_nautical_twilight", "End_nautical_twilight", "solar_noon_Time",
                              "civil_sun_hours", "sun_hours_diff_in_minutes"])

        # Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        # Select only the sunrise and sunset data for the current and next day (specified in PROD/PREPROD CODE)
        
        #PROD. CODE
        if self.mode == "prod":
            current_date = datetime.now().date()
            next_date = current_date + timedelta(days=1)
    

        #DEBUG. CODE
        if self.mode == "debug":
            current_date = datetime(year=2023, month=6, day=26)
            next_date = datetime(year=2023, month=6, day=27)
        #---------------
        
        # Filter sun data to current and next day
        filtered_df = df[
            (df["Year"] == current_date.year)
            & (df["Month"] == current_date.month)
            & (df["Day"] == current_date.day)
            | (df["Year"] == next_date.year)
            & (df["Month"] == next_date.month)
            & (df["Day"] == next_date.day)
        ]

        # Save the filtered sunrise and sunset data to a new CSV file named "sunrise_sunset_processed.csv"
        file_name = "sunrise_sunset_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        filtered_df.to_csv(dst_file, index=False)

    def datetime_to_hours(self, datatime, format="%H:%M:%S"):
        """
        Convert a datetime string to hours with decimals.

        Parameters:
            datetime_str (str): The datetime string to be converted.
            format (str, optional): The format of the input datetime string. Default is "%H:%M:%S".

        Returns:
            float: The converted hours with decimals.
        """

        #Convert datatime to hours 
        time = datetime.strptime(datatime, format)
        total_hours = time.hour + time.minute / 60 + time.second / 3600
        return round(total_hours, 4)
    

    def preprocess_light_prices(self, file_name):
        """
        Preprocess light prices data from the CSV file.

        Parameters:
            file_name (str): The name of the CSV file containing the light prices data.

        Returns:
            None        
        """

        # Read data from the CSV file
        csv_file = os.path.join(self.input_data_path, file_name)
        df = pd.read_csv(csv_file, encoding="latin1")

        # Process date components
        df["Date"] = pd.to_datetime(df["Date"])
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Day"] = df["Date"].dt.day
        
        df["Hour"] = df['hour_range'].str.split().str[0]
        df["Hour"] = pd.to_datetime(df["Hour"], format="%H:%M").dt.time

        # Process light price component
        df["light_price_kwh"] = df['light_price_kwh'].str.split().str[0].astype(float)

        # Delete unrelated variables
        df = df.drop(columns=["hour_range"], axis=1)

        # Sort values by date
        df = df.sort_values(by=["Year", "Month", "Day"])

        # Save the processed light prices data to a new CSV file named "light_prices_processed.csv"
        file_name = "light_prices_processed.csv"
        dst_file = os.path.join(self.temp_data_path, file_name)
        df.to_csv(dst_file, index=False)    
        


    def merge_sky_data(self):
        """
        Merge sky-related dataframes based on common columns.

        Parameters:
            None

        Returns:
            None    
        """

        # Loop through the files in the temp_data_path directory.
        for file_name in os.listdir(self.temp_data_path):
            # Check if the file is a CSV file and does not contain "classified_nodes" or "google_events" in its name.
            if file_name.endswith(".csv") and not "classified_nodes" in file_name and not "google_events" in file_name:
                # Check if the file is related to previous data (contains "previous" in the name) or next data.
                if not "previous" in file_name:
                    # Create the file path and read the CSV file
                    file_path = os.path.join(self.temp_data_path, file_name)
                    other_df = pd.read_csv(file_path)

                    # If "self.df_next" is empty, assign the "other_df" to it.
                    if self.df_next.empty:
                        self.df_next = other_df
                    
                    # Otherwise, merge "other_df" with "self.df_next" based on common columns "Year", "Month", "Day", and "Hour".
                    else:
                        # Check if the "Hour" column exists in both dataframes to determine the type of merge (inner or outer).
                        if "Hour" in self.df_next.columns and "Hour" in other_df.columns:
                            self.df_next = self.df_next.merge(other_df, on=["Year", "Month", "Day", "Hour"], how="inner")
                        
                        else:
                            self.df_next = self.df_next.merge(other_df, on=["Year", "Month", "Day"], how="outer")

                else:
                    # If the file is related to previous data, read it into the "self.df_previous" dataframe
                    file_path = os.path.join(self.temp_data_path, file_name)
                    other_df = pd.read_csv(file_path)
                    self.df_previous = other_df
                
        
    def aggregated_sky_metrics(self,):
        """
        Compute and add aggregated sky-related metrics as new columns to the DataFrame "self.df_next".

        Parameters:
            None

        Returns:
            None
        """

        # Initialize empty lists to store the computed metrics
        upper_light_price_mean = []
        is_night = []
        is_day = []
        needs_artif_light = []
        moon_phase_mult = []

        # Loop through each row in the DataFrame "self.df_next"
        for index, row in self.df_next.iterrows():
            # Compute whether the current row's "light_price_kwh" is higher than the mean of the entire "light_price_kwh" column
            if row["light_price_kwh"] > self.df_next["light_price_kwh"].mean():
                upper_light_price_mean.append(True)
            else:
                upper_light_price_mean.append(False)

            # Check if the current row's "Hour" falls within the range of "Moonset" and "Moonrise" times
            if row["Moonset"] < row["Hour"] < row["Moonrise"]:
                is_night.append(True)
            else:
                is_night.append(False)

            # Check if the current row's "Hour" falls within the range of "Sunset" and "Sunrise" times
            if row["Sunset"] < row["Hour"] < row["Sunrise"]:
                is_day.append(True)
            else:
                is_day.append(False)
            
            # Check if the current row's "Hour" falls within the range of "Start_civil_twilight" and "End_civil_twilight" times
            if row["Start_civil_twilight"] < row["Hour"] < row["End_civil_twilight"]:
                needs_artif_light.append(False)
            else:
                needs_artif_light.append(True)
        
            # Map the "moon_phase" value to a corresponding multiplier (1, 2, or 3)
            if row["moon_phase"] == "New Moon":
                moon_phase_mult.append(1)
            elif row["moon_phase"] == "First Quarter" or row["moon_phase"] == "Third Quarter":
                moon_phase_mult.append(2)
            else: 
                moon_phase_mult.append(3)

        # Add the computed metrics as new columns to the DataFrame "self.df_next"
        self.df_next["upper_light_price_mean"] = upper_light_price_mean
        self.df_next["is_night"] = is_night
        self.df_next["is_day"] = is_day
        self.df_next["needs_artif_light"] = needs_artif_light 
        self.df_next["moon_phase_mult"] = moon_phase_mult
        
    def merge_events_data(self,):
        """
        Merge the data from the "classified_nodes_processed.csv" file and the corresponding Google events data for each zone.
        Store the merged DataFrames for each zone in "self.df_next_list".

        Parameters:
            None

        Returns:
            None        
        """

        # Read the "classified_nodes_processed.csv" file into a DataFrame
        file_path_classified_nodes = os.path.join(self.temp_data_path, "classified_nodes_processed.csv")
        classified_nodes_df = pd.read_csv(file_path_classified_nodes)

        # Loop through each zone in the list of zones "self.zones"
        for zone in self.zones:
            # Read the corresponding Google events data for the current zone
            file_path_google_events = os.path.join(self.temp_data_path, f'google_events_processed_{zone}.csv')
            google_events_zone = pd.read_csv(file_path_google_events)

            # Merge the Google events data with the "self.df_next" DataFrame on the columns "Year", "Month", "Day", and "Hour"
            # If there are no Google events data for the current zone, create a deep copy of "self.df_next" and add NaN values for the missing columns            if len(google_events_zone) > 0:
            if len(google_events_zone) > 0:    
                df_merged = self.df_next.merge(google_events_zone, on=["Year", "Month", "Day", "Hour"], how="left")
            else:
                df_merged = copy.deepcopy(self.df_next)
                events_columns = list(google_events_zone.columns)
                for column in events_columns:
                    if column != "Year" and column != "Month" and column != "Day" and column != "Hour": 
                        df_merged[column] = np.nan
            
            # Add additional columns from the "classified_nodes_df" DataFrame for the current zone to the merged DataFrame "df_merged"
            df_merged["id_list"] = list(classified_nodes_df[classified_nodes_df["zone"] == zone]['id_list'])[0]
            df_merged["type_list"] = list(classified_nodes_df[classified_nodes_df["zone"] == zone]['type_list'])[0]
            df_merged["ebox_id_list"] = list(classified_nodes_df[classified_nodes_df["zone"] == zone]['ebox_id_list'])[0]
            df_merged["coordinates_list"] = list(classified_nodes_df[classified_nodes_df["zone"] == zone]['coordinates_list'])[0]

            # Store the merged DataFrame for the current zone in the list "self.df_next_list"
            self.df_next_list.append((zone, df_merged)) #aqui guardamos el df de esa zona
            

    #SAVE OUTPUT DATA
    def save_output_data(self):
        """
        Save the processed data to CSV files in the output data path.
        Saves the DataFrames from self.df_next_list as separate CSV files based on their corresponding zones.
        Also saves the self.df_previous DataFrame as a single CSV file.
        
        Parameters:
            None

        Returns:
            None
        """

        # Loop through each DataFrame in the "self.df_next_list"
        for df_next in self.df_next_list:
            # Create a file name for the output data based on the zone
            file_name = f'processed_data_next_{df_next[0]}.csv'
            dst_file = os.path.join(self.output_data_path, file_name)

            # Save the current DataFrame to a CSV file in the output data path
            df_next[1].to_csv(dst_file, index=False)

        # Create a file name for the output data for the previous data ("df_previous")
        file_name = "processed_data_previous.csv"
        dst_file = os.path.join(self.output_data_path, file_name)

        # Save the "df_previous" DataFrame to a CSV file in the output data path
        self.df_previous.to_csv(dst_file, index=False)



#DEBUG MAIN
def main():
    preprocessor = Preprocessor(mode="debug", events_source="google")
    preprocessor.get_input_data()
    preprocessor.preprocess_data()
    preprocessor.save_output_data()

#main()


