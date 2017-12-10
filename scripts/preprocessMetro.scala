// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean and merge the metro entrances data

// The data structure after processing should be like:
// RDD[(Int, Double)]
// RDD[(BlockNo, Distance)]

package edu.nyu.bdad.tkyz.utils

import org.apache.spark.rdd.RDD

object PreprocessMetro extends java.io.Serializable{

	val radianConvertor = Math.PI / 180

	// calculate the distance (mile)
	def distance(origin: (Double, Double), target: (Double, Double)): Double = {
		val gap = (Math.abs(target._1 - origin._1), Math.abs(target._2 - origin._2))
		val ds = (gap._1 * 68.6864, gap._2 * Math.cos(origin._1 * radianConvertor) * 69.1710)
		return Math.sqrt(Math.pow(ds._1, 2) + Math.pow(ds._2, 2))
	}

	def run(input: String, blockinput: String): RDD[(Int, Double)] = {
		// val input = "bdad/project/data/cleaned_subway"
		// val blockinput = "/user/yz3940/bdad/project/data/bb"
		
		val blocks = sc.textFile(blockinput, 20).map(e => {
			val block = e.substring(0, e.indexOf(",")).toInt
			var str = e.substring(e.indexOf(",") + 1)
			str = str.substring(str.indexOf(",") + 2)
			str = str.substring(str.indexOf("\"") + 4)
			str = str.substring(0, str.length - 2)
			val splits = str.split(",").map(_.toDouble)
			(block, (splits(1), splits(0)))
		})
		
		val metros = sc.textFile(input, 10).map(e => {
			val str = e.substring(e.indexOf(",") + 1, e.length - 1)
			val geom = str.split(",").map(_.toDouble)
			(geom(1), geom(0))
		})
		
		val metros_list = metros.collect.toList
		val metrosDistances = blocks.mapValues(e => metros_list.map(e1 => distance(e, e1)).reduceLeft(Math.min(_, _)))
		
		return metrosDistances
	}
	
}