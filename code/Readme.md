nyu-bigdata-application-project
===============================

> by Taikun Guo and Yi Zhang

This is our application for NYU big data application course. We have packed our cleaning and profiling code together with our scoring and analyzing code.  

The application majorly read from the datasets first. Then after processing and scoring, it produces the final scores and visualization data to be visualize and analyze.

The application didn't include some of our data cleaning scripts and data fetching scripts. Those scripts are for one time use due to varies reasons.

## Data Source
- Crime data:
  * The crime data was fetched from the NYPD Crime Statistics
  * <https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Map-Historic-/57mv-nv28>
- Metro data:
  * The metro stations data was fetched from MTA
  * <https://data.cityofnewyork.us/Transportation/Subway-Entrances/drex-xx56>
- AQI data:
  * The AQI data was fetched from the AirNow API
  * <https://docs.airnowapi.org/>
- Property values data:
  * The property values data was fetched from the Department of Finance (DOF)
  * <https://data.cityofnewyork.us/Housing-Development/Property-Valuation-and-Assessment-Data/rgy2-tti8>
- BBL data:
  * The BBL data was fetched from DOF and Carto DB
  * <http://gis.nyc.gov/taxmap/map.htm>
  * <http://chriswhong.github.io/plutoplus/>

## To Compile
> The configuration we use:  
- Java 1.7
- scala 2.10
- spark 1.6
- Maven 3.5.2

We use this configuration since Cloudera and Dumbo use Spark 1.6, so we have to set the other softwares to the compatitable versions.

Now build the code using Maven.
```bash
mvn package # maven should download dependencies
```
And the compiled jar file will be `target/uber-finalproject-1.0-SNAPSHOT.jar`.

## To Run
- In hdfs
- Create a folder to store the application data, the path of this folder should be like:
  * ```<path_to_folder>```
- Create a subfolder `data`, the path of the data subfolder should be like: 
  * ```<path_to_folder>/data```
- Place the following files into it:
  * `complaints.csv`
  * `aqi/` (directory)
  * `subway_entrance.csv`
  * `properties_values.csv`
  * `bbls.txt`
- Create a subfolder named `output`.

Now run the application.
```bash
# run the app with 4 cores and 16 G memory.
# <path_to_datasets> is the directory created earlier
spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 \
--class edu.nyu.bdad.tkyz.App --master local[4] --executor-memory 16G \
target/uber-finalproject-1.0-SNAPSHOT.jar <path_to_folder>
```
> The application should run about 10 minutes on Dumbo or more depends on how many RAM.

> The results should be in `output`.

## Results

After running, the application will generate the following files:
- Crime cleaned data:
  * Path: ```<path_to_folder>/output/cleaned_crime```'
  * This is the crime data after cleaning
- Metro cleaned data:
  * Path: ```<path_to_folder>/output/cleaned_subway```
  * This is the metro data after cleaning
- Property values cleaned data:
  * Path: ```<path_to_folder>/output/cleaned_values```
  * This is the property values data after cleaning
- Block geometrics data:
  * Path: ```<path_to_folder>/output/bb```
  * This is the block data with geometric center
- Crime score data:
  * Path: ```<path_to_folder>/output/scores_crime```
  * This is the crime scores data
- Metro score data:
  * Path: ```<path_to_folder>/output/scores_metro```
  * This is the metro scores data
- AQI score data:
  * Path: ```<path_to_folder>/output/scores_aqi```
  * This is the AQI scores data
- Property values score data:
  * Path: ```<path_to_folder>/output/scores_values```
  * This is the property values scores data
- Aggregated raw data:
  * Path: ```<path_to_folder>/output/data_agg_csv```
  * This is the aggregated data containing the block No, crime data, metro data, AQI data, property values data
- Aggregated score data with linear model error:
  * Path: ```<path_to_folder>/output/scores_agg_csv```
  * This is the aggregated data containing the block No, crime scores, metro scores, AQI scores, property values scores, predictions and model errors
- Aggregated score data with lasso linear model error:
  * Path: ```<path_to_folder>/output/scores_agg_csv_lasso```
  * This is the aggregated data containing the block No, crime scores, metro scores, AQI scores, property values scores, predictions and lasso model errors
- Aggregated score data with ridge linear model error:
  * Path: ```<path_to_folder>/output/scores_agg_csv_ridge```
  * This is the aggregated data containing the block No, crime scores, metro scores, AQI scores, property values scores, predictions and ridge model errors
- Model errors score data:
  * Path: ```<path_to_folder>/output/scores_error```
  * This is the aggregated data containing the block No, crime scores, metro scores, AQI scores, property values scores, predictions and model error scores
- Visualization file (KML):
  * Path: ```<path_to_folder>/output/scores_visualization```
  * This is the KML file used to visualize on map

## contacts
Please let us know if any issue comes up.  
* Taikun Guo <taikun@nyu.edu>  
* Yi Zhang <ian.zhang@nyu.edu>
