// Training linear model code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can train the linear model for the property values

package edu.nyu.bdad.tkyz.tasks

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object TrainLinearModel{

	def run(input: String, output: String) {
		// val input = "/user/yz3940/bdad/project/data/scores_agg_csv"
		// val output = "/user/yz3940/bdad/project/data/scores_error_csv"
		
		// delete existed directory
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(output))){
			hdfs.delete(new Path(output), true)
		}
		
		val raw = sc.textFile(input).filter(!_.contains("b")).map(_.split(",")).map(e => (e(0).toDouble.toInt, e(1).toDouble, e(2).toDouble, e(3).toDouble, e(0).substring(0, 1).toDouble, e(4).toDouble))
		
		// train the model
		val iteration = 10 * raw.count.toInt
		val pace = 0.01
		
		val data = raw.map(e => LabeledPoint(e._6, Vectors.dense(Array(e._2, e._3, e._4, e._5)))).cache
		val model = LinearRegressionWithSGD.train(data, iteration, pace)
		
		// evaluate this model by Chi-Squared error
		val errors = raw.map(e => {
			val prediction = model.predict(Vectors.dense(Array(e._2, e._3, e._4, e._5)))
			val observed = e._6
			val error = Math.pow(prediction - observed, 2) / prediction
			(e._1, prediction, error)
		})
		
		data.unpersist(true)
		
		// save the result into a csv file
		val aggregated_rdd = raw.keyBy(_._1).join(errors.keyBy(_._1)).map(e => (e._1, e._2._1._2, e._2._1._3, e._2._1._4, e._2._1._6, e._2._2._2, e._2._2._3))
		(sc.parallelize(List("block,crime,metro,air,propertyValue,prediction,error")) ++ aggregated_rdd.map(e => List(e._1, e._2, e._3, e._4, e._5, e._6, e._7).mkString(","))).saveAsTextFile(output)
	}
}