// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean and merge the AQI data

package edu.nyu.bdad.tkyz.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object PreprocessAQI{

	def main(args: Array[String]) {
		// val input = args[0]
		val input = "/user/yz3940/bdad/project/data/aqi"
		val emptyoutput = "/user/yz3940/bdad/project/data/aqiempty"
		val blockinput = "/user/yz3940/bdad/project/data/bb"
		val blockAqiOutput = "/user/yz3940/bdad/project/data/blockAqi"
		
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(emptyoutput))){
			hdfs.delete(new Path(emptyoutput), true)
		}
		if(hdfs.exists(new Path(blockAqiOutput))){
			hdfs.delete(new Path(blockAqiOutput), true)
		}
		
		val raw = sc.textFile(input).map(_.split(",")).map(e => (e(0).toInt, (e(1).toInt, e(3).toInt)))
		val merge = raw.mapValues(e => List(e)).reduceByKey(_++_).mapValues(_.distinct)
		var yearAvg = merge.mapValues(e => e.groupBy(_._1).map(e1 => (e1._1, e1._2.map(_._2)))).mapValues(e => e.toList.map(e1 => {
			val valid = e1._2.filter(_ != -1)
			if(valid.length == 0)
				(e1._1, -1.0)
			else
				(e1._1, valid.sum.toDouble / valid.length)
		}).sortBy(_._1))
		
		// check whether there is no data for some years
		val empty = yearAvg.mapValues(e => e.filter(_._2 == -1.0)).map(e => e._2.map(e1 => (e._1, e1._1)))
		empty.flatMap(e => e.map(e1 => e1._1 + "," + e1._2)).saveAsTextFile(emptyoutput)
		
		// assign those blocks without AQI data with an average
		val averages = yearAvg.flatMap(e => e._2).groupBy(_._1).map(e => (e._1, e._2.map(_._2))).mapValues(e => e.map(e1 => (1, e1)).reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2))).mapValues(e => e._2 / e._1).collectAsMap
		yearAvg = yearAvg.mapValues(e => (e.filter(_._2 != -1.0) ++ e.filter(_._2 == -1.0).map(e1 => (e1._1, averages(e1._1)))).sortBy(_._1))
		
		// generate air quality data for each block
		val blocks = sc.textFile(blockinput).map(e => {
			val block = e.substring(0, e.indexOf(",")).toInt
			var str = e.substring(e.indexOf(",") + 1)
			str = str.substring(str.indexOf(",") + 2)
			val zips = str.substring(0, str.indexOf("\""))
			var splits = List(0)
			if(!zips.isEmpty)
				splits = zips.split(", ").map(_.toInt).toList
			(block, splits)
		})
		
		val aqiMap = yearAvg.collectAsMap + {0 -> averages.toList}
		val blockAqi = blocks.mapValues(e => e.map(e1 => aqiMap(e1)).reduceLeft(_++_).groupBy(_._1).map(e1 => {
			val tmp = e1._2.map(e2 => (1, e2._2)).reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2))
			(e1._1, tmp._2.toDouble / tmp._1)
		}).toList.sortBy(_._1))
		blockAqi.flatMap(e => e._2.map(e1 => e._1 + "," + e1._1 + "," + format("%.4f", e1._2))).saveAsTextFile(blockAqiOutput)
	}	

}