// Profling code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can profile the crimes and property values data

package edu.nyu.bdad.tkyz.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Profile {
	
	def run(crimePath: String, valuesPath: String, outputPathPrefix: String) {
		
		//val crimePath = "/user/yz3940/bdad/project/data/complaints.csv"
		//val valuesPath = "/user/yz3940/bdad/project/data/properties_values.csv"
		//val outputPathPrefix = "/user/yz3940/bdad/project/data/profile_report"
		
		val c_output = outputPathPrefix + "_crime"
		val v_output = outputPathPrefix + "_values"
		
		// delete existed directory
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(c_output))){
			hdfs.delete(new Path(c_output), true)
		}
		if(hdfs.exists(new Path(v_output))){
			hdfs.delete(new Path(v_output), true)
		}
		
		val c_df = sqlContext.load("com.databricks.spark.csv", Map("path" -> crimePath, "header" -> "true"))
		val v_df = sqlContext.load("com.databricks.spark.csv", Map("path" -> valuesPath, "header" -> "true"))
		
		// profile the crime data
		val result_c = profile(c_df)
		sc.parallelize(c_df.columns.zip(result_c)).saveAsTextFile(c_output)
		
		// profile the values data
		val result_v = profile(v_df)
		sc.parallelize(v_df.columns.zip(result_v)).saveAsTextFile(v_output)
		
	}

	def profile(df: DataFrame) : Array[String] = {
		val columns = df.columns
		val columnNum = columns.length - 1
		val result = new Array[String](columnNum + 1)
		
		for(i <- 0 to columnNum){
			val column = df.rdd.map(_.get(i).toString)
			column.cache
			val filtered = column.filter(!_.isEmpty)
			var first : String = null
			if(filtered.count == 0){
				result(i) = "Empty column"
			} else {
				first = filtered.first
				if(Try(first.toInt).isSuccess){
					val numbers = column.filter(e => (Try(e.toInt).isSuccess)).map(_.toInt)
					val range = numbers.map(e => (e, e)).reduce((x, y) => (Math.min(x._1, y._1), Math.max(x._2, y._2)))
					result(i) = "Range: " + range
				} else if(Try(first.toDouble).isSuccess){
					val numbers = column.filter(e => (Try(e.toDouble).isSuccess)).map(_.toDouble)
					val range = numbers.map(e => (e, e)).reduce((x, y) => (Math.min(x._1, y._1), Math.max(x._2, y._2)))
					result(i) = "Range: " + range
				} else{
					val max = column.map(_.length).reduce((x, y) => Math.max(x, y))
					
					val distinct = column.distinct
					if(distinct.count < 10){
						result(i) = "Max length: " + max + "; Elements: [" + distinct.collect.mkString(", ") + "]"
					} else{
						val sample = distinct.takeSample(false, 10, System.nanoTime.toInt)
						result(i) = "Max length: " + max + "; Samples: [" + sample.mkString(", ") + "]"
					}
				}
			}
			column.unpersist(false)
		}
		
		return result
	}
}