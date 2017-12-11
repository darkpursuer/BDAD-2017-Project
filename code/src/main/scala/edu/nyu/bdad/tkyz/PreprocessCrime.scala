// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean and merge the crime data

// The data structure after processing should be like:
// RDD[(Int, List[(Int, Int)])]
// RDD[(BlockNo, List[(Year, TotalCrimeScore)])]

package edu.nyu.bdad.tkyz

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object PreprocessCrime extends java.io.Serializable{

  // check whether the target point is in range of the origin
  def inRange(radius: Double, radianConvertor: Double, latDeg: Double, origin: (Double, Double), target: (Double, Double)): Boolean = {
    val longDeg = radius / (Math.cos(origin._1 * radianConvertor) * 69.1710)
    val gap = (Math.abs(target._1 - origin._1), Math.abs(target._2 - origin._2))
    return gap._1 <= latDeg && gap._2 <= longDeg
  }

  def run(sContext: SparkContext, d: Double, input: String, blockinput: String): RDD[(Int, List[(Int, Int)])] = {

    // basic parameters
    val sc = sContext
    val radius = d  // mile
    val latDeg = radius / 68.6864
    val radianConvertor = Math.PI / 180
    val crimeScore = Map("MISDEMEANOR" -> 1, "VIOLATION" -> 2, "FELONY" -> 3)

    val rdd = sc.textFile(input, 100).map(e => {
      val splits = e.split(",")
      val dateStr = splits(1)
      val year = dateStr.substring(dateStr.lastIndexOf("/") + 1).toInt
      val crimeType = splits(3)
      val latitude = splits(6).toDouble
      val longitude = splits(7).toDouble
      ((latitude, longitude), (year, crimeScore(crimeType)))
    })

    val blocks = sc.textFile(blockinput, 20).map(e => {
      val block = e.substring(0, e.indexOf(",")).toInt
      var str = e.substring(e.indexOf(",") + 1)
      str = str.substring(str.indexOf(",") + 2)
      str = str.substring(str.indexOf("\"") + 4)
      str = str.substring(0, str.length - 2)
      val splits = str.split(",").map(_.toDouble)
      ((splits(1), splits(0)), block)
    })

    val blockGeoms = blocks.collect
    val blockScores = rdd.map(e => (blockGeoms.filter(e1 => inRange(radius, radianConvertor, latDeg, e1._1, e._1)).map(_._2).toList, e._2)).flatMap(e => e._1.map(e1 => (e1, List(e._2)))).reduceByKey(_++_).mapValues(e => e.groupBy(_._1).map(e1 => (e1._1, e1._2.map(_._2).reduceLeft(_+_))).toList)

    return blockScores
  }
}
