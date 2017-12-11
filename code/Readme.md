nyu-bigdata-application-project
===============================

> by Taikun Guo and Yi Zhang

This is our application for NYU big data application course. We have packed our cleaning and profiling code together with our scoring and analyzing code.  

The application majorly read from the datasets first. Then after processing and scoring, it produces the final scores and visualization data to be visualize and analyze.

The application didn't include some of our data cleaning scripts and data fetching scripts. Those scripts are for one time use due to varies reasons.

## To Compile
> First you need to set your Scala version to `2.11` and Java version to `1.8`. And make sure you have Maven installed.

Now build the code using Maven.
```bash
mvn package # maven should download dependencies
```
And the compiled jar file will be `target/uber-finalproject-1.0-SNAPSHOT.jar`.

## To Run
- Before run, make sure Scala version is `2.11` and Java is `1.8`.  
- Create a folder to store datasets, create a subfolder `data` and place the following files into it:
  * `complaints.csv`
  * `aqi`
  * `subway_entrance.csv`
  * `properties_values.csv`
  * `bbls.txt`
- Create a subfolder named `output`.

Now run the application.
```bash
scala -classpath target/uber-finalproject-1.0-SNAPSHOT.jar edu.nyu.bdad.tkyz.App <the_path_of_the_datasets>
```
The results should be in `output`.
