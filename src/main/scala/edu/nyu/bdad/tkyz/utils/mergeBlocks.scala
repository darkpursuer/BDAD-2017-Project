// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean and merge the block geom data

package edu.nyu.bdad.tkyz.utils

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object MergeBlocks{

	val sc = new SparkContext

	def main(args: Array[String]) {

		// preprocess and merge the BBL data
		val input = args(0)
		val bb_output = args(1)
		
		// val input = "/user/yz3940/bdad/project/data/bbl_raw.txt"
		// val bb_output = "/user/yz3940/bdad/project/data/bb"

		// delete existed directory
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(bb_output))){
			hdfs.delete(new Path(bb_output), true)
		}

		// BBL format: BBLNo B area zip coordinates
		// BBLNo format: B[1]B[5]L[4]
		val bblraw = sc.textFile(input).map(_.split(" ")).keyBy(_(0))

		// remove the duplicate entries
		val bbls = bblraw.reduceByKey((x, y) => x)
		// bbls.cache

		// val duplicates = bbls.filter(e => e._2(2).contains(";") || e._2(3).contains(";") || e._2(4).contains(";"))

		// aggregate the geo region in the same block
		def normalize(point: (Double, Double)): Double = {
			return Math.sqrt(Math.pow(point._1, 2) + Math.pow(point._2, 2))
		}

		def cosine(standard: (Double, Double), test: (Double, Double)): Double = {
			if((standard._1 == 0.0 && standard._2 == 0.0) || (test._1 == 0.0 && test._2 == 0.0)){
				return 2.0
			}
			val left = normalize(standard)
			val right = normalize(test)
			val face = normalize((test._1 - standard._1, test._2 - standard._2))
			return (Math.pow(left, 2) + Math.pow(right, 2) - Math.pow(face, 2)) / (2 * left * right)
		}

		// arg1: coordinates
		// return: List((latitude, langitude))
		def aggregateBlockGeo(coordinates: String): List[(Double, Double)] = {
			val cords = coordinates.split("\\)").map(_.substring(1).split(",")).map(e => (e(0).toDouble, e(1).toDouble))
			
			// find a border point: ymin
			var start = cords.map(e => (e._2, e)).reduceLeft((x, y) => {
				var ymin: Double = 0.0
				var yminl: (Double, Double) = x._2
				if(x._1 > y._1){
					ymin = y._1
					yminl = y._2
				} else{
					ymin = x._1
					yminl = x._2
				}
				(ymin, yminl)
			})._2
			
			var result = ListBuffer.empty[(Double, Double)]
			
			var standard = (-1.0, 0.0)
			var next = (0.0, 0.0)
			var current = start
			var i = cords.length
			
			while(!(next._1 == start._1 && next._2 == start._2) && i > 0){
				next = cords.map(e => (cosine(standard, (e._1 - current._1, e._2 - current._2)), e)).reduceLeft((x, y) => {
					if(x._1 < y._1){
						x
					} else{
						y
					}
				})._2
				
				result += next
				standard = (current._1 - next._1, current._2 - next._2)
				current = next
				i = i - 1
			}
			
			return result.toList
		}

		def getMiddlePoint(borders: List[(Double, Double)]): (Double, Double) = {
			var totalArea, totalX, totalY = 0.0
			val N = borders.length
			for(i <- 0 to N - 1){
				val a = borders((i + 1) % N)
				val b = borders(i)
				val area = 0.5 * (a._1 * b._2 - b._1 * a._2)
				totalArea = totalArea + area
				totalX = totalX + area * (a._1 + b._1) / 3
				totalY = totalY + area * (a._2 + b._2) / 3
			}
			return (totalX / totalArea, totalY / totalArea)
		}

		// aggregate those in the same block
		val blocks = bbls.map(e => (e._1.substring(0, 6), e._2)).mapValues(e => {
			var zip = List[Int]()
			if(e(3) != "0"){
				zip = List(e(3).toInt)
			}
			(e(2).toInt, zip, e(4))
		}).reduceByKey((x, y) => {
			(x._1 + y._1, (x._2 ++ y._2).distinct, x._3 + y._3)
		}).mapValues(e => (e._1, e._2, getMiddlePoint(aggregateBlockGeo(e._3))))

		// save the blocks information to HDFS
		blocks.map(e => List(e._1, e._2._1.toString, "\"" + e._2._2.mkString(", ") + "\"", "\"(" + e._2._3._1 + "," + e._2._3._2 + ")\"").mkString(",")).saveAsTextFile(bb_output)
	}

}