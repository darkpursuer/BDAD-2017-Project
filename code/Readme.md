nyu-bigdata-application-project
===============================

> by Taikun Guo and Yi Zhang

This is our application for NYU big data application course. We have packed our cleaning and profiling code together with our scoring and analyzing code.  

The application majorly read from the datasets first. Then after processing and scoring, it produces the final scores and visualization data to be visualize and analyze.

The application didn't include some of our data cleaning scripts and data fetching scripts. Those scripts are for one time use due to varies reasons.

## To Compile
> The configuration we use:  
- Java 1.7
- scala 2.10
- spark 1.6
- Maven 3.5.2

We use this configuration since Cloudera use Spark 1.6, so we have to set the other softwares to the compatitable versions.

Now build the code using Maven.
```bash
mvn package # maven should download dependencies
```
And the compiled jar file will be `target/uber-finalproject-1.0-SNAPSHOT.jar`.

## To Run
- In hdfs
- Create a folder to store datasets, create a subfolder `data` and place the following files into it:
  * `complaints.csv`
  * `aqi`
  * `subway_entrance.csv`
  * `properties_values.csv`
  * `bbls.txt`
- Create a subfolder named `output`.

Now run the application.
```bash
spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 \
--class edu.nyu.bdad.tkyz.App --master local  \
target/uber-finalproject-1.0-SNAPSHOT.jar <path_to_datasets>
```
> The application should run about 10 minutes on Dumbo or more depends on how many RAM.

> The results should be in `output`.

## Results
After running the application

## contacts
Please let us know if any issue comes up.  
* Taikun Guo <taikun@nyu.edu>  
* Yi Zhang <yz3940@nyu.edu>
