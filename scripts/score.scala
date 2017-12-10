// Scoring code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can score the crime, metro and air quality scores

package edu.nyu.bdad.tkyz.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object Score extends java.io.Serializable{
	
	// the function to normalize values to (0, 1)
	def normalize(value: Double, alpha: Double): Double = {
		return Math.pow(Math.E, -1 * alpha * value) * (alpha * value + 1)
	}
	
	// the sigmoid function to normalize the property values to (0, 1)
	def sigmoid(value: Double, alpha: Double): Double = {
		return 2.0 / (1 + Math.pow(Math.E, -1 * alpha * value)) - 1
	}
	
	def run(crimes_raw: RDD[(Int, List[(Int, Int)])], metros: RDD[(Int, Double)], aqis_raw: RDD[(Int, List[(Int, Double)])], values: RDD[(Int, Double)], outputPrefix: String, agg_d_output: String, agg_s_output: String): List[String] = {
		// val outputPrefix = "bdad/project/data/scores"
		
		val crimeOutput = outputPrefix + "_crime"
		val metroOutput = outputPrefix + "_metro"
		val aqiOutput = outputPrefix + "_aqi"
		val valuesOutput = outputPrefix + "_values"
		
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(crimeOutput))){
			hdfs.delete(new Path(crimeOutput), true)
		}
		if(hdfs.exists(new Path(metroOutput))){
			hdfs.delete(new Path(metroOutput), true)
		}
		if(hdfs.exists(new Path(aqiOutput))){
			hdfs.delete(new Path(aqiOutput), true)
		}
		if(hdfs.exists(new Path(valuesOutput))){
			hdfs.delete(new Path(valuesOutput), true)
		}
		if(hdfs.exists(new Path(agg_d_output))){
			hdfs.delete(new Path(agg_d_output), true)
		}
		if(hdfs.exists(new Path(agg_s_output))){
			hdfs.delete(new Path(agg_s_output), true)
		}
		
		/* Start scoring crimes */
		
		// load crime data
		val crimes = crimes_raw.mapValues(_.map(e1 => (1, e1._2.toDouble)).reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2))).mapValues(e => e._2 / e._1)
		
		// scoring parameters 
		val crime_avg_tmp = crimes.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val crime_avg = crime_avg_tmp._2.toDouble / crime_avg_tmp._1
		val alpha_c = 2.0 / crime_avg
		val scores_c = crimes.mapValues(e => normalize(e, alpha_c))
		
		// save crime scores
		println("    Saving Crime scores into " + crimeOutput)
		scores_c.map(e => e._1 + "," + "%.4f".format(e._2)).saveAsTextFile(crimeOutput)
		
		/* Start scoring Aqis */
		
		// load Aqi data
		val aqis = aqis_raw.mapValues(_.map(e1 => (1, e1._2.toDouble)).reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2))).mapValues(e => e._2 / e._1)
		
		// scoring parameters 
		val aqi_avg_tmp = aqis.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val aqi_avg = aqi_avg_tmp._2.toDouble / aqi_avg_tmp._1
		val alpha_a = 2.0 / aqi_avg
		val scores_a = aqis.mapValues(e => normalize(e, alpha_a))
		
		// save Aqi scores
		println("    Saving AQI scores into " + aqiOutput)
		scores_a.map(e => e._1 + "," + "%.4f".format(e._2)).saveAsTextFile(aqiOutput)
		
		/* Start scoring metro entrances */
		val metro_avg_tmp = metros.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val metro_avg = metro_avg_tmp._2.toDouble / metro_avg_tmp._1
		val alpha_m = 2.0 / metro_avg
		val scores_m = metros.mapValues(e => normalize(e, alpha_m))
		
		// save metro scores
		println("    Saving Metro scores into " + metroOutput)
		scores_m.map(e => e._1 + "," + "%.4f".format(e._2)).saveAsTextFile(metroOutput)
		
		/* Start scoring property values */
		val values_avg_tmp = values.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val values_avg = values_avg_tmp._2.toDouble / values_avg_tmp._1
		val alpha_v = 1.0 / values_avg
		val scores_v = values.mapValues(e => sigmoid(e, alpha_v))
		
		println("    Saving Property Values scores into " + valuesOutput)
		scores_v.map(e => e._1 + "," + "%.4f".format(e._2)).saveAsTextFile(valuesOutput)
		
		// save the aggregated raw data
		val aggregated_rdd = crimes.join(metros).join(aqis).join(values).map(e => (e._1, e._2._1._1._1, e._2._1._1._2, e._2._1._2, e._2._2))
		(sc.parallelize(List("block,crime,metro,air,propertyValue")) ++ aggregated_rdd.map(e => List(e._1, e._2, e._3, e._4, e._5).mkString(","))).saveAsTextFile(agg_d_output)
		
		// save the aggregated score data
		val agg_s_rdd = scores_c.join(scores_m).join(scores_a).join(scores_v).map(e => (e._1, (e._2._1._1._1, e._2._1._1._2, e._2._1._2, e._2._2)))
		(sc.parallelize(List("block,crimeScore,metroScore,airScore,valueScore")) ++ agg_s_rdd.map(e => List(e._1, e._2._1, e._2._2, e._2._3, e._2._4).mkString(","))).saveAsTextFile(agg_s_output)
		
		return List(crimeOutput, metroOutput, aqiOutput, valuesOutput)
	}

	def scoreError(input: String, outputPrefix: String): String = {
		// val input = "/user/yz3940/bdad/project/data/scores_error_csv"
		// val outputPrefix = "bdad/project/data/scores"
		
		val output = outputPrefix + "_error"
		
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(output))){
			hdfs.delete(new Path(output), true)
		}
		
		val raw = sc.textFile(input).filter(!_.contains("b")).map(_.split(",")).map(e => (e(0).toDouble.toInt, e(6).toDouble))
		
		val avg_tmp = raw.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val avg = avg_tmp._2.toDouble / avg_tmp._1
		val alpha = 1.0 / avg
		val scores = raw.mapValues(e => sigmoid(e, alpha))
		
		println("    Saving Errors scores into " + output)
		scores.map(e => e._1 + "," + "%.4f".format(e._2)).saveAsTextFile(output)
		
		return output
	}
}