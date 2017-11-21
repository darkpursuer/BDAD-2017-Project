package edu.nyu.bdad.tkyz.tasks

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.sql._

object ExtractDistinctZipcodes extends AbstractTask {
  val sc: SparkContext = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val BLOCKS_CSV_PATH = "BLOCK_CSV_PATH"
  val OUTPUT_PATH = "ZIPCODE_OUTPUT_PATH"

  override def execute(): Unit = {
    val blocks_csv = sqlContext.read.format("csv").option("header", "true").load(BLOCKS_CSV_PATH)
    val blocks_rdd = blocks_csv.rdd
    // get all the unique zip codes
    val zips = blocks_rdd.map(e => e(2).toString).flatMap(e => e.split(", ")).distinct().filter(_.nonEmpty)
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    if(hdfs.exists(new Path(OUTPUT_PATH))){
      hdfs.delete(new Path(OUTPUT_PATH), true)
    }
    zips.toDF("zipcode").write.format("csv").option("header", "true").save(OUTPUT_PATH)
  }
}
