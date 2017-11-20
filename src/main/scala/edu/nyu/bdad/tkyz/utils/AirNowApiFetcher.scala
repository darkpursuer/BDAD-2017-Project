/***
 * Created for 2017 Fall NYU Big Data Application Development course.
 * Copyright 2017 Taikun Guo & Yi Zhang
 * 
 * This is a utility class that used to fetch
 * air quality data from AirNow
 * https://docs.airnowapi.org/webservices
 */
package edu.nyu.bdad.tkyz.utils

import scala.util.parsing.json._
import scala.io.Source
import java.net.{URL, HttpURLConnection}

// a bunch of classes that used to extract a JSON object
class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object M extends CC[Map[String, Any]]
object L extends CC[List[Any]]
object S extends CC[String]
object D extends CC[Double]

/**
 * An utility class that fetch history data of Air Quality Index(AQI) based on PM2.5
 * Using date and geo location
 */
object AirNowApiFetcher {

  // the base API url, return format is set to json
  // this is hard coded, may need to change if the api is changed
  val baseUrl = "http://www.airnowapi.org/aq/observation/latLong/historical?format=application/json"
  // the api key of the service
  // the service is currently free, but we do need to register an account to have a key
  val apiKey = "COPY_API_KEY_HERE"

  /**
   * Input: year : Int
   *        month : Int
   *        day : Int
   *        lat : Double
   *        lon : Double
   * Output: AQI : Double (0.0 - 100.0)
                   or -1.0 if no AQI with PM2.5 found
   * Throws: java.io.IOException
   *         java.net.SocketTimeoutException
   */
  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def getAQIByDateAndGeo(year: Int, month: Int, day: Int, lat: Double, lon: Double): Double = {
    // parse date to a 'yyyy-mm-ddT00-0000' string
    val date = f"$year%04d-$month%02d-$day%02dT00-0000"
    // the api only takes 4 digits after float point
    val latitude = f"$lat%.4f"
    val longitude = f"$lon%.4f"
    // construct the url
    val url = baseUrl + s"&latitude=$latitude&longitude=$longitude&date=$date&API_KEY=$apiKey"

    // set up connection
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(10000) // set timeout to both 10 seconds
    connection.setReadTimeout(10000)
    connection.setRequestMethod("GET") // this only works with GET method
    val inputStream = connection.getInputStream
    val content = Source.fromInputStream(inputStream).mkString // call url
    if (inputStream != null) inputStream.close()
    
    // parse json string into an object
    // which is a list of tuples
    // each tuple contain (AQI, paramater)
    // we need to keep the one with parameter = PM2.5
    val results = for {
      Some(L(items)) <- List(JSON.parseFull(content.stripMargin))
      M(item) <- items
      D(aqi) = item("AQI")
      S(parameter) = item("ParameterName")
    } yield {
      (aqi, parameter)
    }

    // return the one with PM2.5
    for (r <- results) {
      if (r._2 == "PM2.5") {
        return r._1
      }
    }
    // return -1 if no PM2.5 AQI found
    -1.0
  }

  // def main(args: Array[String]): Unit = {
  //   // some calls for testing
  //   println(getAQIByDateAndGeo(2017, 11, 17, 40.7855, -73.9683))
  // }

}