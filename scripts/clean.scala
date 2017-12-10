// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean the crimes and property values data
package edu.nyu.bdad.tkyz.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Clean extends java.io.Serializable{

	def run(subFile: String, crimePath: String, valuesPath: String, outputPathPrefix: String): List[String] = {

		// val crimePath = "/user/yz3940/bdad/project/data/complaints.csv"
		// val valuesPath = "/user/yz3940/bdad/project/data/properties_values.csv"
		// val outputPathPrefix = "/user/yz3940/bdad/project/data/cleaned"
		
		val s_output = outputPathPrefix + "_subway"
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
		if(hdfs.exists(new Path(s_output))){
			hdfs.delete(new Path(s_output), true)
		}
		
		val s_csv = sqlContext.load("com.databricks.spark.csv", Map("path" -> subFile, "header" -> "true"))
		val c_df = sqlContext.load("com.databricks.spark.csv", Map("path" -> crimePath, "header" -> "true"))
		val v_df = sqlContext.load("com.databricks.spark.csv", Map("path" -> valuesPath, "header" -> "true"))
		
		// clean the crime data
		// Complaint_Num, Date, KY_CD, Type, Borough, Address PCT CD, Latitude, Longitude
		val c_needIndex = Array(0, 5, 6, 11, 13, 14, 21, 22)
		clean(c_df, c_needIndex).map(_.mkString(",")).saveAsTextFile(c_output)
		
		// clean the values data
		// BBLE, District, Year, CUR_FV_L, CUR_FV_T, NEW_FV_L, NEW_FV_T, ZipCode, LND_AREA, GR_SQFT
		val v_needIndex = Array(0, 6, 7, 8, 9, 10, 11, 48, 62, 63)
		clean(v_df, v_needIndex).map(_.mkString(",")).saveAsTextFile(v_output)
		
		// convert into RDDs
		val s_data = s_csv.rdd

		// clean
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
		s_filtered.saveAsTextFile(s_output)
		
		return List(s_output, c_output, v_output)
	}

	def clean(df: DataFrame, indices: Array[Int]) : RDD[Array[String]] = {
		val filtered = df.rdd.map(e => indices.map(e1 => e.get(e1).toString))
		val nonempty = filtered.filter(e => {
			var hasEmpty = false
			var i = 0
			while(!hasEmpty && i < e.length){
				hasEmpty = e(i).isEmpty
				i = i + 1
			}
			!hasEmpty
		})
		nonempty
	}
}