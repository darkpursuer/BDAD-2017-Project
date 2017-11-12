// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// Profile air quality data and subway data

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io._

object ProfileAirSubway {

  def profileFiles(airFile: String, subFile: String, outputPathPrefix: String) {
    // set output paths
    val a_output = outputPathPrefix + "_air"
		val s_output = outputPathPrefix + "_subway"

    // clean exist files
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(a_output))){
			hdfs.delete(new Path(a_output), true)
		}
		if(hdfs.exists(new Path(s_output))){
			hdfs.delete(new Path(s_output), true)
		}

    // load files
    val a_csv = sqlContext.load("com.databricks.spark.csv", Map("path" -> airFile, "header" -> "true"))
		val s_csv = sqlContext.load("com.databricks.spark.csv", Map("path" -> subFile, "header" -> "true"))

    // convert into RDDs
    val a_data = a_csv.rdd
    val s_data = s_csv.rdd

    // profile
    val a_filtered = a_data.filter(
      e =>
        e(1).toString.trim.toInt == 662 // keep only pm2.5
        && e(4).toString.trim == "UHF42" // keep UHF42 area data
    ).map(
      e => (
        e(5), // geo_entity_id
        e(6), // geo_entity_name
        e(8) // value
      )
    )
    val a_geo_entity_ids = a_filtered.collect.map(e => e._1.toString.trim.toInt)
    val a_values = a_filtered.collect.map(e => e._3.toString.trim.toFloat)
    var a_profile = List("Air Quality Data Profle")
    a_profile = a_profile.:+(" ")
    a_profile = a_profile.:+("Columns:")
    a_profile = a_profile.:+("geo_entity_id Int - The location entity id")
    a_profile = a_profile.:+("geo_entity_name String - The location description")
    a_profile = a_profile.:+("data_valuemessage Float - The average pm 2.5 concentration value")
    a_profile = a_profile.:+(" ")
    a_profile = a_profile.:+("All geo_entity_ids:")
    for (item <- a_geo_entity_ids) {
      a_profile = a_profile.:+(item.toString)
    }
    a_profile = a_profile.:+(" ")
    a_profile = a_profile.:+("value range:")
    a_profile = a_profile.:+(("max = " + a_values.max.toString))
    a_profile = a_profile.:+(("min = " + a_values.min.toString))

    var s_profile = List("Subway data profile")
    s_profile = s_profile.:+(" ")
    s_profile = s_profile.:+("Columns:")
    s_profile = s_profile.:+("OBJECTID Int - The id of the table")
    s_profile = s_profile.:+("the_geom coordinates - the geo coordinates of each subway station")
    s_profile = s_profile.:+(" ")
    s_profile = s_profile.:+(("Number of stations: " + s_data.count.toString))

    // store
    sc.parallelize(a_profile).saveAsTextFile(a_output)
    sc.parallelize(s_profile).saveAsTextFile(s_output)
  }
}