import logging
import urllib3
from selenium.webdriver.remote.remote_connection import LOGGER as selenium_logger

# Desactivar los mensajes de registro de Selenium, scrapy, urllib3 y hyperopt
logging.getLogger('selenium').setLevel(logging.WARNING)
selenium_logger.setLevel(logging.WARNING)
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
urllib3_logger = logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('filelock').setLevel(logging.ERROR)
logging.getLogger('hyperopt').setLevel(logging.WARNING)

from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

#utils
import os
from datetime import datetime, timedelta
import csv
import pandas as pd
import math
import numpy as np

#predict imports
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
#regression models
from sklearn.linear_model import LinearRegression
from lightgbm import LGBMRegressor
from xgboost.sklearn import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.linear_model import SGDRegressor
from sklearn.kernel_ridge import KernelRidge
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import BayesianRidge
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.svm import SVR

#hyperparameter tuning
from hyperopt import fmin, tpe, hp
from hyperopt.pyll.base import scope



class HistoricalLightPriceSpider(Spider):
    name = 'historical_light_price'
    allowed_domains = ['tarifaluzhora.es']
    start_urls = ['https://tarifaluzhora.es']

    def __init__(self, project_root, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.project_root = project_root

        #Save actual date and next day date
        self.dates = []

        self.historical_df = None
        
        #PROD. CODE
        self.act_date = datetime.now()

        #Define data headers
        self.header = ["Date", "hour_range", 'light_price_kwh' ]

        #Allowed domains and start url
        self.allowed_domains = ['tarifaluzhora.es']
        self.start_urls = ['https://tarifaluzhora.es']


    def start_requests(self):

        self.delete_csv()

        for url in self.start_urls:
            date_extract = self.act_date + timedelta(days=1)
            for i in range(30):
                date_extract = date_extract - timedelta(days=1)
                date_extract_str = date_extract.strftime("%Y-%m-%d")

                #Get the url of light price on the indicated day
                url_parse = "/".join([url, f"?date={date_extract_str}"])
                yield Request(url_parse, callback=self.parse, meta={'Date': date_extract_str})

        
    def parse(self, response):
        
        #Create list to save the extracted data
        light_prices_historical = []

        for row in response.xpath('//div[@class="template-tlh__colors--hours"]'): #Select element that contains the row info
                    hour_range = row.xpath('.//div//span/text()')[0].get() #Select element with hour_range info
                    price = row.xpath('.//div//span/text()')[1].get() #Select element with light price
                    date = response.meta["Date"] #Get date from meta-response

                    data = dict(zip(self.header, [date, hour_range, price])) #Crate a dict with header - key , data extracted - value
                    light_prices_historical.append(data) #Add row dict to data list
        

        self.save_csv(light_prices_historical, response.meta["Date"])
        

    def delete_csv(self,):
        file_path = os.path.join(self.project_root, "LightPrice_Spiders", "data", "light_prices_historical.csv")
        file_exists = os.path.isfile(file_path)
        if file_exists:
            os.remove(file_path)

        
    def save_csv(self, light_prices, date):
        file_path = os.path.join(self.project_root, "LightPrice_Spiders", "data", "light_prices_historical.csv")

        file_exists = os.path.isfile(file_path) #Check if file exists
        
        # Open the CSV file in append mode to add new rows
        with open(file_path, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.header)

            # Write the header if the file doesn't exist
            if not file_exists:
                writer.writeheader()

            # Write all the rows
            writer.writerows(light_prices)

        # Read the CSV file using pandas
        df = pd.read_csv(file_path)

        # Convert the 'hour_range' column to a datetime format for sorting
        df['hour_range'] = pd.to_datetime(df['hour_range'].str.split('-').str[0].str.strip(), format='%H:%M')
        
        # Sort the data by 'Date' and 'hour_range' columns in ascending order
        df_sorted = df.sort_values(by=['Date', 'hour_range'], ascending=[True, True])
 
        # Convert the 'hour_range' column back to its original format
        df_sorted['hour_range'] = df_sorted['hour_range'].dt.strftime('%H:%M - ') + (df_sorted['hour_range'] + pd.DateOffset(hours=1)).dt.strftime('%H:%M')
        
        # Write the sorted data back to the CSV file
        df_sorted.to_csv(file_path, index=False)




class LightPricePredictor():
    def __init__(self, project_root):
        self.historical_df = None

        self.X = None
        self.y = None

        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        
        self.project_root = project_root


    def get_input_data(self,):
        historical_file_path = os.path.join(self.project_root, "LightPrice_Spiders", "data", "light_prices_historical.csv")
        self.historical_df = pd.read_csv(historical_file_path)


    def preprocess_df(self,):
         # Convertir la columna 'hour_range' a un valor numérico (por ejemplo, tomando el valor medio)
        self.historical_df['hour_start'] = self.historical_df['hour_range'].apply(lambda x: (int(x.split(':')[0]) + int(x.split(':')[1].split()[0])) / 2)

        # Convertir la columna 'Date' a un valor númerico (día)
        self.historical_df['day'] = self.historical_df['Date'].apply(lambda x: (int(x.split('-')[-1])))

        # Convertir la columna 'light_price_kwh' a float eliminando caracteres no numéricos
        self.historical_df['light_price_kwh'] = self.historical_df['light_price_kwh'].str.replace('[^\d.]', '', regex=True).astype(float)
    
    
    def split_df(self,):
        # Dividir el DataFrame en características (X) y etiquetas (y)
        self.X = self.historical_df[['hour_start', 'day']]
        self.y = self.historical_df['light_price_kwh']

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.X, self.y, test_size=0.2, random_state=42)


    
    def select_best_model_arch(self,):
        # Create and train with different regression models
        models = [LinearRegression(), LGBMRegressor(), XGBRegressor(), SGDRegressor(), BayesianRidge(), SVR()]
        best_model = (None, math.inf)
        for model in models:
            
            model.fit(self.X_train, self.y_train)

            # Calculate evaluation metrics
            rmse = np.sqrt(mean_squared_error(self.y_test, model.predict(self.X_test)))

            if rmse < best_model[1]:
                best_model = (model, rmse)

        print("Best model architecture: ", best_model[0].__class__.__name__)
        print(f'RMSE: {best_model[1]}')

        return best_model[0].__class__.__name__


    def hyperparameter_tuning(self, best_architecture):
        def objective(params):
            model_class = globals()[best_architecture]
            model = model_class(**params)
            
            model.fit(self.X_train, self.y_train)
            
            rmse = np.sqrt(mean_squared_error(self.y_test, model.predict(self.X_test)))
            
            return rmse
        
        # Define hyperparameter search space for XGBRegressor
        default_params = {
            'max_depth': 3,
            'reg_alpha': 0,
            'reg_lambda': 1,
            'colsample_bytree': 1,
            'min_child_weight': 1,
            'n_estimators': 100,
            'learning_rate': 0.1,
            'random_state': 0,
        }

        # Define hyperparameter search space for XGBRegressor
        space = {
            'max_depth': hp.choice("max_depth", [default_params['max_depth'] - 1, default_params['max_depth'], default_params['max_depth'] + 1]),
            'reg_alpha': hp.uniform('reg_alpha', 0, default_params['reg_alpha'] + 1),
            'reg_lambda': hp.uniform('reg_lambda', 0, default_params['reg_lambda'] + 10),
            'colsample_bytree': hp.uniform('colsample_bytree', default_params['colsample_bytree'] - 0.1, 1),
            'min_child_weight': hp.uniform('min_child_weight', default_params['min_child_weight'] - 1, default_params['min_child_weight'] + 1),
            'n_estimators': default_params['n_estimators'],
            'learning_rate': hp.uniform('learning_rate', default_params['learning_rate'] - 0.05, default_params['learning_rate'] + 0.05),
            'random_state': default_params['random_state'],
        }

        best_params = fmin(fn=objective,
                           space=space,
                           algo=tpe.suggest,
                           max_evals=100,
                           show_progressbar=False)  # You can adjust max_evals as needed


        best_model = globals()[best_architecture](**best_params)
        best_model.fit(self.X_train, self.y_train)
        best_rmse = np.sqrt(mean_squared_error(self.y_test, best_model.predict(self.X_test)))

        print(f'Best hyperparameters: {best_params}')
        print(f'Best RMSE: {best_rmse}')

        return best_model
    

    def predict(self, model):
        X_predict = pd.DataFrame({'hour_start': [i + 0.5 for i in range(0, 24)], 'day': [int((datetime.now()).day)+1 for i in range(0, 24)]})
        predictions = model.predict(X_predict)

        # Crear una lista para las predicciones
        predictions_list = []

        date_tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        for hour, prediction in enumerate(predictions):
            start_hour = f'{hour:02}:00'
            end_hour = f'{(hour + 1) % 24:02}:00'
            hour_range = f'{start_hour} - {end_hour}'
            prediction_info = {
                'Date': date_tomorrow,
                'hour_range': hour_range,
                'light_price_kwh': f'{prediction:.5f} €/kWh'
            }
            predictions_list.append(prediction_info)

        print("Tomorrow light price predictions DONE.")

        return predictions_list


    def predict_tomorrow_light_price(self):
        #Get historical light price df (input data) 
        self.get_input_data()

        # Adapt the df to be able to make predictions
        self.preprocess_df()

        # Dividir el DataFrame en características (X) y etiquetas (y)
        self.split_df()

        # Search for the best architecture
        best_architecture = self.select_best_model_arch()

        #Search for better hyperparameters for the best architecture
        best_model = self.hyperparameter_tuning(best_architecture)

        # Predict prices for the next day with the best model and save in a list of dict
        predictions_list = self.predict(best_model)

        return predictions_list


            


def main(project_root):
    process = CrawlerProcess(get_project_settings())
    process.crawl(HistoricalLightPriceSpider, project_root)
    process.start()

    light_price_predictor = LightPricePredictor(project_root)
    predictions_list = light_price_predictor.predict_tomorrow_light_price()

    return predictions_list

#main(project_root=r"c:\Users\abelb\Desktop\Gamification - main test")