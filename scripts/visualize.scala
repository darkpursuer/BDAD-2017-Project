// Visualizing code for BDAD project
// Copyright 2017 Yi Zhang and Taikun Guo
// This code can convert the crime, metro and air quality scores to KML file to visualize them

package edu.nyu.bdad.tkyz.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ListBuffer

object Visualize extends java.io.Serializable{
	
	def run(crimeInput: String, metroInput: String, aqiInput: String, propertyInput: String, errorInput: String, bblGeomInput: String, output: String) {
		
		val hdfs = FileSystem.get(sc.hadoopConfiguration)
		if(hdfs.exists(new Path(output))){
			hdfs.delete(new Path(output), true)
		}
		
		val crime_s = sc.textFile(crimeInput).map(_.split(",")).map(e => (e(0).toInt, e(1).toDouble))
		val metro_s = sc.textFile(metroInput).map(_.split(",")).map(e => (e(0).toInt, e(1).toDouble))
		val aqi_s = sc.textFile(aqiInput).map(_.split(",")).map(e => (e(0).toInt, e(1).toDouble))
		val properties = sc.textFile(propertyInput).map(_.split(",")).map(e => (e(0).toInt, e(1).toDouble))
		val errors = sc.textFile(errorInput).map(_.split(",")).map(e => (e(0).toInt, e(1).toDouble))
		
		val aggregated = crime_s.join(metro_s).join(aqi_s).join(properties).map(e => (e._1, (e._2._1._1._1, e._2._1._1._2, e._2._1._2, e._2._2))).join(errors).map(e => (e._1, (e._2._1._1, e._2._1._2, e._2._1._3, e._2._1._4, e._2._2))).collectAsMap
		
		// "bdad/project/data/bbls.txt"
		val bblGeom = sc.textFile(bblGeomInput, 20).map(_.split(" ")).map(e => (e(0), e(0).substring(0, 6).toInt, e(4).split("\\)").map(e => e.substring(1, e.length)).mkString(" ")))
		val indexedbbl = bblGeom.zipWithIndex.mapValues(e => (e + 1).toInt)
		 
		val fields = List(("cartodb_id", "int"), ("block", "int"),("crimeScore", "float"),("metroScore", "float"),("airScore", "float"),("propertyValue", "float"),("error", "float"))
		
		val contents = ListBuffer.empty[String]
		contents += "<?xml version=\"1.0\" encoding=\"utf-8\" ?>"
		contents += "<kml xmlns=\"http://www.opengis.net/kml/2.2\">"
		contents += "<Document id=\"root_doc\">"
		
		contents += "<Schema name=\"scores\" id=\"scores\">"
		val simplefield = "<SimpleField name=\"%s\" type=\"%s\"></SimpleField>"
		contents ++= fields.map(e => simplefield.format(e._1, e._2))
		contents += "</Schema>"
		contents += "<Folder><name>scores</name>"
		
		val c_rdd = sc.parallelize(contents)
		
		val marks = indexedbbl.map(e => {
			if(aggregated.exists(_._1 == e._1._2))
				(e._2, e._1._1, e._1._2, aggregated(e._1._2), e._1._3)
			else
				(e._2, e._1._1, e._1._2, (0.0, 0.0, 0.0, 0.0, 0.0), e._1._3)
		}).flatMap(e => {
			val buffer = ListBuffer.empty[String]
			buffer += "<Placemark>"
			buffer += "<ExtendedData><SchemaData schemaUrl=\"#scores\">"
			buffer += "<SimpleData name=\"cartodb_id\">%d</SimpleData>".format(e._1)
			buffer += "<SimpleData name=\"block\">%d</SimpleData>".format(e._3)
			buffer += "<SimpleData name=\"crimeScore\">%.4f</SimpleData>".format(e._4._1)
			buffer += "<SimpleData name=\"metroScore\">%.4f</SimpleData>".format(e._4._2)
			buffer += "<SimpleData name=\"airScore\">%.4f</SimpleData>".format(e._4._3)
			buffer += "<SimpleData name=\"propertyValue\">%.4f</SimpleData>".format(e._4._4)
			buffer += "<SimpleData name=\"error\">%.4f</SimpleData>".format(e._4._5)
			buffer += "</SchemaData></ExtendedData>"
			buffer += "<MultiGeometry><Polygon><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></MultiGeometry>".format(e._5)
			buffer += "</Placemark>"
			buffer.toList
		})
		
		val total = c_rdd ++ marks ++ sc.parallelize(List("</Folder></Document></kml>"))
		total.saveAsTextFile(output)
	}
}