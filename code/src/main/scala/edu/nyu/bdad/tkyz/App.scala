/** *
  * Created for 2017 Fall NYU Big Data Application Development course.
  * Copyright 2017 Taikun Guo & Yi Zhang
  *
  * Entry point of the project
  */

package edu.nyu.bdad.tkyz

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object App {

  def main(args: Array[String]) {
    // create spark context
    // use local for now
    val conf = new SparkConf().setAppName("BDADProject").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // input arguments
    val pathPrefix = args(0)
    val crimePath = pathPrefix + "/data/complaints.csv"
    val aqiPath = pathPrefix + "/data/aqi"
    val metroPath = pathPrefix + "/data/subway_entrance.csv"
    val valuesPath = pathPrefix + "/data/properties_values.csv"
    val bblGeomInput = pathPrefix + "/data/bbls.txt"

    // output path
    val outputPathPrefix = pathPrefix + "/output/cleaned"
    val block_output = pathPrefix + "/output/bb"
    val s_outputPrefix = pathPrefix + "/output/scores"
    val v_output = pathPrefix + "/output/scores_visualization"
    val agg_d_output = pathPrefix + "/output/data_agg_csv"
    val agg_s_output = pathPrefix + "/output/scores_agg_csv"
    val error_output = pathPrefix + "/output/scores_error_csv"

    // clean the data
    println("Start cleaning the data sets...")
    val outputs = Clean.run(sc, metroPath, crimePath, valuesPath, outputPathPrefix)

    // merge the blocks data
    println("Start merging the blocks data sets...")
    MergeBlocks.run(sc, bblGeomInput, block_output)

    // preprocess the data
    println("Start preprocessing the crime data sets...")
    val radius_c = 0.5
    val crimes = PreprocessCrime.run(sc, radius_c, outputs(1), block_output)

    println("Start preprocessing the AQI data sets...")
    val aqis = PreprocessAQI.run(sc, aqiPath, block_output)

    println("Start preprocessing the metro data sets...")
    val metros = PreprocessMetro.run(sc, outputs(0), block_output)

    println("Start preprocessing the property values data sets...")
    val values = PreprocessProperty.run(sc, outputs(2), bblGeomInput)

    // score the data
    println("Start scoring the data...")
    val s_outputs = Score.run(sc, crimes, metros, aqis, values, s_outputPrefix, agg_d_output, agg_s_output)

    // train the model
    println("Start training the model...")
    TrainLinearModel.run(sc, agg_s_output, error_output)

    // socre the error_output
    println("Start scoring the error...")
    val error_score = Score.scoreError(sc, error_output, s_outputPrefix)

    println("Start visualizing the data...")
    Visualize.run(sc, s_outputs(0), s_outputs(1), s_outputs(2), s_outputs(3), error_score, bblGeomInput, v_output)
  }

}
