// Cleaning code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can clean and merge the property values data

// The data structure after processing should be like:
// RDD[(Int, Double)]
// RDD[(BlockNo, Value)]

package edu.nyu.bdad.tkyz.utils

import org.apache.spark.rdd.RDD

object PreprocessProperty extends java.io.Serializable{

	def run(input: String, bblGeomInput: String): RDD[(Int, Double)] = {
		// val input = "bdad/project/data/cleaned_values"
		
		val raw = sc.textFile(input, 20).map(_.split(",")).map(e => (e(0), (e(3).toDouble, e(8).toDouble)))
		
		val bblGeom = sc.textFile(bblGeomInput, 20).map(_.split(" ")).map(e => (e(0), e(2).toDouble))
		
		val refinedValues = bblGeom.join(raw).mapValues(e => {
			var area = e._2._2
			if(area < 0.000001)
				area = e._1
			(e._2._1, area)
		}).map(e => (e._1.substring(0, 6).toInt, e._2))

		val values = refinedValues.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(e => {
			if(e._2 < 0.000001)
				0.0
			else
				e._1 / e._2
		})
		return values
	}
}