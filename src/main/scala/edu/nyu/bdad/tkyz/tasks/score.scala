// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can score the crime, metro and air quality scores

package edu.nyu.bdad.tkyz.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object score{
	
	// the function to normalize values to (0, 1)
	def normalize(value: Double, alpha: Double): Double = {
		return Math.pow(Math.E, -1 * alpha * value) * (alpha * value + 1)
	}
	
	def run(crimeInput: String, metroInput: String, aqiInput: String, blockinput: String, outputPrefix: String){
		val crimeInput = "bdad/project/data/cleaned_crime"
		val metroInput = "bdad/project/data/cleaned_subway"
		val aqiInput = "bdad/project/data/aqi"
		val blockinput = "/user/yz3940/bdad/project/data/bb"
		val outputPrefix = "bdad/project/data/scores"
		
		val crimeOutput = outputPrefix + "_crime"
		val metroOutput = outputPrefix + "_metro"
		val aqiOutput = outputPrefix + "_aqi"
		
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
		
		/* Start scoring crimes */
		
		// load crime data
		val radius_c = 0.5
		val pc = new PreprocessCrime(radius_c)
		val crimes: RDD[(Int, List[(Int, Int)])] = pc.run(crimeInput, blockinput)
		
		// scoring parameters 
		val crime_avg_tmp = crimes.flatMap(_._2.map(e => (1, e._2))).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val crime_avg = crime_avg_tmp._2.toDouble / crime_avg_tmp._1
		val alpha_c = 2.0 / crime_avg
		val scores_c = crimes.mapValues(e => e.map(e1 => (e1._1, normalize(e1._2, alpha_c))))
		
		// save crime scores
		scores_c.flatMap(e => e._2.map(e1 => e._1 + "," + e1._1 + "," + format("%.4f", e1._2))).saveAsTextFile(crimeOutput)
		
		/* Start scoring Aqis */
		
		// load Aqi data
		val aqis: RDD[(Int, List[(Int, Double)])] = PreprocessAQI.run(aqiInput, blockinput)
		
		// scoring parameters 
		val aqi_avg_tmp = aqis.flatMap(_._2.map(e => (1, e._2))).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val aqi_avg = aqi_avg_tmp._2.toDouble / aqi_avg_tmp._1
		val alpha_a = 2.0 / aqi_avg
		val scores_a = aqis.mapValues(e => e.map(e1 => (e1._1, normalize(e1._2, alpha_a))))
		
		// save Aqi scores
		scores_a.flatMap(e => e._2.map(e1 => e._1 + "," + e1._1 + "," + format("%.4f", e1._2))).saveAsTextFile(aqiOutput)
		
		/* Start scoring metro entrances */
		val metros: RDD[(Int, Double)] = PreprocessMetro.run(metroInput, blockinput)
		
		val metro_avg_tmp = metros.map(e => (1, e._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
		val metro_avg = metro_avg_tmp._2.toDouble / metro_avg_tmp._1
		val alpha_m = 2.0 / metro_avg
		val scores_m = metros.mapValues(e => normalize(e, alpha_m))
		
		// save metro scores
		scores_m.map(e => e._1 + "," + e._2).saveAsTextFile(metroOutput)
	}

}