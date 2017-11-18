// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// Clean air quality data and subway data

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object CleanAirSubway {

  def cleanFiles(airFile: String, subFile: String, outputPathPrefix: String) {
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

    // clean
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

    val s_filtered = s_data.filter(
      e =>
        (!e(0).toString.trim.isEmpty)
        && e(3).toString.trim.matches("""POINT \([0-9.\-]+ [0-9.\-]+\)""")
    ).map(
      e => (
        e(0), 
        e(3).toString.split("""\(""")(1).split("""\)""")(0).split(" ")(0).toDouble, 
        e(3).toString.split("""\(""")(1).split("""\)""")(0).split(" ")(1).toDouble
      )
    )

    // store
    a_filtered.saveAsTextFile(a_output)
    s_filtered.saveAsTextFile(s_output)
  }

}
