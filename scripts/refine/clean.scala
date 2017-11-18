// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean the crimes and property values data

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Clean {

	def main(args: Array[String]){
		val crimePath = args(0)
		val valuesPath = args(1)
		val outputPathPrefix = args(2)
		
		// val crimePath = "/user/yz3940/bdad/project/data/complaints.csv"
		// val valuesPath = "/user/yz3940/bdad/project/data/properties_values.csv"
		// val outputPathPrefix = "/user/yz3940/bdad/project/data/cleaned"
		
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
		
		// clean the crime data
		val c_needIndex = Array(0, 5, 6, 11, 13, 14, 21, 22)
		clean(c_df, c_needIndex).saveAsTextFile(c_output)
		
		// clean the values data
		val v_needIndex = Array(0, 6, 7, 8, 9, 10, 11, 48, 63)
		clean(v_df, v_needIndex).saveAsTextFile(v_output)
	}

	def clean(df: DataFrame, indices: Array[Int]) : RDD[Any] = {
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