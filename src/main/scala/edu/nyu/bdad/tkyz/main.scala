// Main process for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code is the main entrance of our project

package edu.nyu.bdad.tkyz

// load reference codes
:load /home/yz3940/bdad/pj/codes/clean.scala
:load /home/yz3940/bdad/pj/codes/mergeBlocks.scala
:load /home/yz3940/bdad/pj/codes/preprocessCrime.scala
:load /home/yz3940/bdad/pj/codes/preprocessMetro.scala
:load /home/yz3940/bdad/pj/codes/preprocessAQI.scala
:load /home/yz3940/bdad/pj/codes/preprocessProperty.scala
:load /home/yz3940/bdad/pj/codes/score.scala
:load /home/yz3940/bdad/pj/codes/trainLinearModel.scala
:load /home/yz3940/bdad/pj/codes/visualize.scala

val crimePath = "/user/yz3940/bdad/project/data/complaints.csv"
val aqiPath = "/user/yz3940/bdad/project/data/aqi"
val metroPath = "/user/yz3940/bdad/project/data/subway_entrance.csv"
val valuesPath = "/user/yz3940/bdad/project/data/properties_values.csv"
val bblGeomInput = "/user/yz3940/bdad/project/data/bbls.txt"
val inputArgs = Array(crimePath, aqiPath, metroPath, valuesPath, bblGeomInput)

object Main{

	def start(args: Array[String]){
		
		val crimePath = args(0)
		val aqiPath = args(1)
		val metroPath = args(2)
		val valuesPath = args(3)
		val bblGeomInput = args(4)
		
		val outputPathPrefix = "/user/yz3940/bdad/project/data/cleaned"
		val block_output = "/user/yz3940/bdad/project/data/bb"
		val s_outputPrefix = "/user/yz3940/bdad/project/data/scores"
		val v_output = "/user/yz3940/bdad/project/data/scores_visualization"
		val agg_d_output = "/user/yz3940/bdad/project/data/data_agg_csv"
		val agg_s_output = "/user/yz3940/bdad/project/data/scores_agg_csv"
		val error_output = "/user/yz3940/bdad/project/data/scores_error_csv"
		
		// clean the data
		println("Start cleaning the data sets...")
		val outputs = Clean.run(metroPath, crimePath, valuesPath, outputPathPrefix)
		
		// merge the blocks data
		println("Start merging the blocks data sets...")
		MergeBlocks.run(bblGeomInput, block_output)
		
		// preprocess the data
		println("Start preprocessing the crime data sets...")
		val radius_c = 0.5
		val pc = new PreprocessCrime(radius_c)
		val crimes = pc.run(outputs(1), block_output)
		
		println("Start preprocessing the AQI data sets...")
		val aqis = PreprocessAQI.run(aqiPath, block_output)
		
		println("Start preprocessing the metro data sets...")
		val metros = PreprocessMetro.run(outputs(0), block_output)
		
		println("Start preprocessing the property values data sets...")
		val values = PreprocessProperty.run(outputs(2), bblGeomInput)
		
		// score the data
		println("Start scoring the data...")
		val s_outputs = Score.run(crimes, metros, aqis, values, s_outputPrefix, agg_d_output, agg_s_output)
		
		// train the model
		println("Start training the model...")
		TrainLinearModel.run(agg_s_output, error_output)
		
		// socre the error_output
		println("Start scoring the error...")
		val error_score = Score.scoreError(error_output, s_outputPrefix)
		
		println("Start visualizing the data...")
		Visualize.run(s_outputs(0), s_outputs(1), s_outputs(2), s_outputs(3), error_score, bblGeomInput, v_output)
	}
}