# nyu-bigdata-application-project
A study on the relationship between housing prices and the air quality, metro stations and crimes in NYC

## Useful Links

[NYC BBL Map](http://gis.nyc.gov/taxmap/map.htm)

[BBL Info Download](http://chriswhong.github.io/plutoplus)

[UHF42 to zip code](https://www.health.ny.gov/statistics/cancer/registry/appendix/neighborhoods.htm)

## Note
In order to do `maven package`, we need to set `JAVA_HOME` point to `1.7`.
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 1.7`
```

To run the scala codes, we need to enter the spark shell by 
```bash
$ spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
```
