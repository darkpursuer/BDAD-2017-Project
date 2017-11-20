/***
 * Created for 2017 Fall NYU Big Data Application Development course.
 * Copyright 2017 Taikun Guo & Yi Zhang
 * 
 * Fetch AirQualityIndex from AirNow
 * From 2010 - 2017
 * Calculate yearily average
 */
package edu.nyu.bdad.tkyz.tasks

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.joda.time._

object FetchAirQualityIndex extends AbstractTask {
  
  val sc: SparkContext = new SparkContext
  val sqlContext = new SQLContext(sc)

  val BLOCKS_CSV_PATH = "SET_PATH_HERE"
  val OUTPUT_FOLDER = "SET_AIR_QUALITY_FOLDER"

  override def execute(): Unit = {
    // load cleaned blocks file into RDD
    val blocks_csv = sqlContext.read.format("csv").option("header", "true").load(BLOCKS_CSV_PATH)
    val blocks_rdd = blocks_csv.rdd
    // map data => (BlockNo, (lat, lon))
    val blocks_processed = blocks_rdd.map(e => 
      (e(0), (e(3).toString.split(",")(1).split(")")(0).toDouble, // latitude
          e(3).toString.split(",")(0).split("(")(1).toDouble))) // longitude
    // for each of them hit api and get historical air quality
    // 
  }

//  def getHistoricalAQI(): List[Double] = {
//
//  }
//
  def getYearAverage(year: Int, lat: Double, lon: Double): Double = {
    val from = new DateTime(year, 1, 1, 0, 0) // the start of this year
    val til = new DateTime(year, 12, 31, 0, 0) // the end of this year
    val daysCount = Days.daysBetween(from, til).getDays() // number of days between
    val dates = (0 until daysCount).map(from.plusDays(_).toString("YYYY-mm-dd"))
    
    
    
    1.0
  }
  
  def main(args: Array[String]): Unit = {
    val from = new DateTime(2016, 1, 1, 0, 0) // the start of this year
    val til = new DateTime(2016, 12, 31, 0, 0) // the end of this year
    val daysCount = Days.daysBetween(from, til).getDays() // number of days between
    val dates = (0 until daysCount).map(from.plusDays(_).toString("YYYY-mm-dd"))
  }

}