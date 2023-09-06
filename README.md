# Documentation gamification

This is the repository for the implementation of the predictive maintenance project for INNERGY.

## Description of the project:

The main objective of the project is the creation of a module with the help of Web Scraping, Machine Learning and Artificial Intelligence techniques capable of recommending the recommended light intensity for the next day of the public lights of a city in a given context. The objective when giving intensity recommendations is to reduce it in those moments that the context allows it and thus achieve energy and CO2 savings. The repository contains the entire process, from the extraction of the context (weather data, electricity prices, events) to the calculation of recommendations and graphs of energy and CO2 savings. The code has excellent comments at each step, ensuring a clear understanding of each module.

The recommender will take as input information about the weather from the last 3 days (to determine, for example, whether the ground will be covered in snow), as contextual data for the next day. These contextual data are made up of: meteorological data (weather, rain, clouds, moon phase, lunar and solar lighting), electricity price and events in the city on the day to be recommended. The results of the recommender are: recommended intensity, energy savings and CO2 savings.

* **Contextual Data Extractor**

  * **Sky Info Web scraping:** The extraction of data related to the sky is carried out from various Web Scrapers

    * **Meteo web scraping:** Gather the meteorological information from a certain municipality over a specified time period using web scraping.
    * **Moon phases web scraping**: Gather the information on the lunar phases to find out if the moon is going to improve the city's nighttime lighting.
    * **Moonrise and moonset web scraping**: Obtain data related to the time of moonrise and moonset to know which time ranges it will affect and we have to consider the characteristics of the moon.
    * **Sunrise and sunset web scraping:** Obtain data related to the time of sunrise and sunset to know in which time ranges there will be natural light and it is not necessary to use artificial light in the city.
  * **Events Extractor:** It is used to obtain the list of events scheduled in the city. There are currently two ways to obtain these events:

    * **Google Events web scraping:** Extracts city events that have been published on Google and have been collected by the Google Events tool (engine).
    * **Events Generator**: False event generator using artificial intelligence (ChatGPT). It is important to carry out tests in cities where, due to their small size, not many events are held.
  * **Light Prices Web scraping:** Gather the price of electricity over a specified time period using web scraping.
* **Node Classifier:** The Node Classifier module is an essential part of our project that is used for classifying nodes in a municipality based on their geographical location. This allows identifying in which specific zone of the municipality each node is located, which is essential for the management and control of lighting systems, event monitoring and other related applications.
* **Data preprocessing:** Pipeline to do the necessary data preprocessing of the raw breakdown data, the raw nodes data and the scrapped contextual data.
* **Light Intensity Recommender:** Recommend light intensity levels based on contextual factors and calculate energy and CO2 savings.
* **Visualizer**: Chart creator to visualize the intensity recommender results.

Let's explain step-by-step how each one of the modules works:

*"It is important to note that although the modules can be executed separately, the idea is to execute the main program, which executes all the modules sequentially and is designed to be able to configure the recommender by introducing input arguments.  After explaining how the modules work separately, you will be able to find out how the main module works and how it can be executed."*

## **Contextual Data Extractor**

* **Sky Info Web scraping:** All Web Scrapers that extract sky related information are located in the "SkyInfo_Spiders" folder. Inside this folder, apart from the Web Scrapers, you can find a folder called "data" where the extracted data is saved in CSV format.

  All of these Web Scrapers accept an input parameter called "mode". Which accepts two types of mode: "debug" and "prod". The "debug" mode is used to test the code and you can select days in the past to make recommendations. The "prod" mode is the real mode, which collects current data to make recommendations for tomorrow.

  * **Meteo web scraping:** The Meteo Web Scraper is located in "spiders/meteo.py". Running this module, the weather data available at wunderground.com is scraped and saved in two CSVs. Both files store the temperature, dew point, humidity percentage, cloud percentage, wind speed, atmospheric pressure, amount of rain and weather description (where you can know if it is sunny, rainy, snowy, cloudy, etc. ) of each hour of the selected period. The first csv is called "weather_previous.csv" and saves this weather data for the last 72 hours, the second is called "weather_next.csv" and saves the data for the next 24 hours.
  * **Moon phases web scraping**: The Moon Phases Web Scraper is located in "spiders/moon_phases.py". This scraper connects to the timeanddate.com page and downloads the calendar of each lunar phase in csv format in order to know the current lunar phase. Once the module is executed, you can find the output csv in the "data" folder with the name "moon_phases.csv".
  * **Moonrise and moonset web scraping**: The Moonrise and Moonset Web Scraper is located in "spiders/moonrise_moonset.py". This scraper connects to the timeanddate.com page and extracts information related to the moonrise and moonset for each day of the month (we really only need the day to recommend, but the page groups the data monthly, therefore in the preprocessing data module we will have to filter by recommended day). In addition to extracting the times of moonset and moonrise, information is also collected about its illumination and distance from Earth. You can find the response csv in the "data" folder with the name "moonrise_moonset.csv".
  * **Sunrise and sunset web scraping:** The Sunrise and Sunset Web Scraper is located in "spiders/sunrise_sunset.py". This module works the same as the previous one, but includes the time of sunrise and sunset, length of day, hours that activities can be done without the need for artificial light, etc. You can find the response csv in the "data" folder with the name "sunrise_sunset.csv".
* **Events Extractor:** This section contains the different modules used to extract contextual information about events in a municipality. At this moment, when running the main program (which will be explained later) you can choose between extracting this information using a Google Events scraper or using a fake event generator with the help of ChatGPT.

  * **Google Events web scraping:** The Google Events Web Scraper is located in "RSS_Spiders/RSS_Spiders/spiders/google_events.py". This module runs a scraper that is responsible for extracting information from tomorrow's events of a certain municipality published in Google Events and saving them in CSV format. The information that is saved is: title, schedule, location and description. You can find the response csv in the "data" folder with the name "google_events.csv".
  * **Event Generator**: The Event Generator is located in "Event_generator". This module uses queries to ChatGPT to generate false events at different times and locations in the indicated municipality. The result is saved in a CSV in the same way as in the module explained above. You can find the response csv in the "data" folder with the name "fake_events.csv".
* **Light Prices Web scraping:** The Google Events Web Scraper is located in "LightPrice_Spiders/LightPrice_Spiders/spiders/light_price.py". This module is a scraper that is responsible for accessing tarifaluzhora.es and extracting the electricity price for the current day and the next day (day to recommend the light intensity) per hour. You can find the response csv in the "data" folder with the name "light_prices.csv".

## Node Classifier

You can find the Node Classifier module inside the "Node_classifier" folder. This folder is made up of the "input_data" folder that saves a  CSV with the geolocation of each node and a json with the geodelimitation of each zone of the municipality, "output_data" that saves the data once classified and the file "node_classifier.py" which contains a class with the necessary methods to classify the input nodes.

## Data Preprocessing

You can find the Data Preprocessing module inside the ""Preprocessor"" folder. This folder is made up of the "input_data" folder that saves the input data, "temp_data" that saves the data in an intermediate preprocessing phase, "output_data" that saves the data once preprocessed and the file "preprocessor.py" which contains a class with the necessary methods to preprocess the input data.

This module is explained step by step below:

- First, all the CSVs generated in the Contextual Data Extractor section are collected and saved in the "input_data" folder
- Next, these files are then preprocessed individually. The objective is to achieve an optimal format to be able to work with this data later. You can consult the code comments to learn more about the preprocessing. The resulting CSVs are saved in "temp_data"
- Afterwards, all the individual preprocessed dataframes are merged and the information is separated into multiple dataframes. The first dataframe saves the information of the last 72 hours (contextual information of the common past for the entire municipality) and the other dataframes save the information of the next 24 hours for each zone of the city (a dataframe has been created for each area already that each area has a different context of events).
- Finally, dataframes are saved in CSV format in "output_data" folder with name "processed_data_next_{zone_name}.csv" and "processed_data_previous.csv".

## Light Intensity Recommender

You can find the Light Intensity Recommender module inside the "Light_intensity_recommender" folder. This folder is made up of the "input_data" folder that saves the preprocessed data, "output_data" that saves the data with the recommendations of intensity, intensity savings and CO2 by zone and the file "Light_intensity_recommender.py" which contains a class with the necessary methods to make recommendations.

This module is explained step by step below:

- First, get the input data from the "output_data" folder of Preprocessor module and copy it to the "input_data" folder of the Light Intensity Recommender module.
- Next, calculate recommended light intensity levels based on various factors, such as events, light prices, moon phases, weather conditions (snow, rain, clouds) and other time and zone-specific parameters. You can choose which factors to use by indicating them in the `params` input argument. Additionally, calculate the energy savings for each zone and time based on the difference between the recommended light intensity and the real light intensity.
- In the same way, calculate the actual and recommended consumption of carbon dioxide (CO2) based on the real and recommended light intensity. There are also parameters such as power in kilowatts, hours and CO2 emission factor that influence the calculation. Additionally, calculate CO2 savings based on the differences between actual and recommended CO2 consumption.
- Finally, save the results in separate CSV files for each zone and time, including recommended light intensity, actual light intensity and energy savings. It also stores a summary of savings by zone in a CSV file.

## **Visualizer**

You can find the Visualizer module inside the "Visualizer" folder. This folder is made up of the "input_data" folder that saves the light intensity recommendations data, "output_data" that saves the the different plots generated as images and the file "Visualizer.py" which contains a class with the necessary methods to display actual light intensity data versus recommended intensity and save the plots as images.

Below is a description of the key aspects of this module:

- First, get input data from "output_data" folder of "Light Intensity Recommender" module and copy it to "input_data" folder of "Visualizer" module.
- Next, display the actual intensity versus the recommended intensity for each zone/lighting and store the corresponding graphs. You can enter the input argument ` type ` which can be "bar" for a bar chart or "scatter" for a scatter chart. Additionally, display the intensity savings for each zone and create a bar graph showing the percentage savings for each zone.
- Finally, save the generated plots as images in the "output_data" folder of the visualizer.

## Gamification Main Program

The main program that coordinates the execution of all the modules explained above to perform the gamification of light intensity.  You can find the main module of the program inside the "Gamification" folder with the name main.py.

The main program is distributed this way:

- First, initiates processes to run the scrapies to collect weather data, moon phase information, moon and sun rise and set times, and electricity prices.
- Next, run the node classifier to assign events to specific locations.
- Then, preprocesses the extracted data for subsequent analysis and recommendations.
- Afterward, run the light intensity recommender to calculate light intensity recommendations and other related metrics.
- Finally, generates visualizations of the results if argument `plot_results` is True.

**Necessary libraries**

In order to run the program successfully, it is necessary to have the necessary libraries. To do this, it is advisable to create an environment and install the libraries from the requirements.txt file.

**Configuration parameters and run examples**

* `--mode`: Indicates the execution mode (debug or prod).
  * Possible values:
    * `debug`: Debugging mode.
    * `prod`: Production mode (default).
  * Example: `--mode debug`
* `--events_source`: Specifies the event source to use.
  * Possible values:
    * `google`: Get events from Google Events (default).
    * `generator`: Generate fake events with ChatGPT
  * Example: `--events_source generator`
* `--recommender_params`: Allows you to select the recommender parameters.
  * Possible values: List of characteristics separated by spaces.
    * `light price`: Price of electricity.
    * `moon`: Moon phases.
    * `snow`: Snow conditions.
    * `rain`: Rainy conditions.
    * `cloud`: Cloud cover.
  * Example: `--recommender_params "light price" "moon" "rain"`
* `--no_plot_results`: Disables the generation of result visualizations (by default, enabled).
  * Example: `--no_plot_results`



You can run the main program with the default parameters (prod mode, Google Events events source and all the recommender params.) in the terminal as follows:

```
python main.py
```

Running in debug mode, source of fake events generated by ChatGPT, specific selection of recommender parameters and without plotting results:

```
python main.py --mode debug --events_source generator --recommender_params "light_price" "moon" "rain" --no_plot_results
```

## Extra: Scheduling Daily Executions with Apache Airflow

We have implemented automated scheduling of daily runs of the main Gamification program using Apache Airflow. This allows us to execute tasks on a scheduled and reliable basis, which is essential for sending daily, updated recommendations to our platform.

We have defined a DAG called 'gamification' that encapsulates all the programming logic of our daily tasks. Here is a description of some of the key aspects of this setup:

- default_args: We have configured the default values that will be applied to all tasks in the DAG. This includes the DAG owner, start time (20:30 CET), number of retry attempts, and retry interval.
- schedule_interval: We have set a daily execution scheduling interval using schedule_interval=timedelta(days=1). This means that the DAG will run once a day.
- task_gamification: We have defined a task called 'execute_gamification' that uses the BashOperator operator. This task will execute a Bash command that starts the main Gamification program running.

*In order to use Apache Airflow scheduled executions, it is necessary to start a webserver and scheduler and activates DAGs on a own server.*
