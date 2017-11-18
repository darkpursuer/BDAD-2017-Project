/***
 * Created for 2017 Fall NYU Big Data Application Development course.
 * Copyright 2017 Taikun Guo & Yi Zhang
 * 
 * This file contains some util methods
 * related to geo calculation
 */
package edu.nyu.bdad.tkyz.utils

import scala.math._

class GeoPoint(latitude: Double, longitude: Double) {
  // getters
  def lat(): Double = this.latitude
  def lon(): Double = this.longitude
}

object GeoUtils {

  /**
   * Convert from degree difference to radius difference
   */
  def degToRad(deg: Double): Double = deg * (Pi/180)

  /**
   * Using Haversine formula
   * https://en.wikipedia.org/wiki/Haversine_formula
   * to calculate the distance between two geo point
   * in km
   */
  def distanceKm(p1: GeoPoint, p2: GeoPoint): Double = {
    val R = 6371 // radius of Earth in km
    val dLat = degToRad(p1.lat - p2.lat)
    val dLon = degToRad(p1.lon - p2.lon)
    val a = pow(asin(dLat/2), 2.0) + pow(asin(dLon/2), 2.0) * acos(degToRad(p1.lat)) * acos(degToRad(p2.lat))
    val c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c
  }

  /**
   * From a set of points, find out how many of them
   * are within the given radius of the given source point.
   */
  def countWithinRadius(source: GeoPoint, points: Array[GeoPoint], radius: Double): Int = {
    var count = 0
    for (p <- points) {
      if (distanceKm(p, source) <= radius) count+= 1
    }
    return count
  }

  /**
   * From the given source to any point in points.
   * Find the shortest distance in Km
   */
  def shortestDistance(source: GeoPoint, points: Array[GeoPoint]): Double = {
    var distance = Double.PositiveInfinity
    for (p <- points) {
      var curDis = distanceKm(p, source)
      if (curDis < distance) {
        distance = curDis
      }
    }
    return distance
  }


  // def main(args: Array[String]): Unit = {
  //   //println(distanceKm(new GeoPoint(38.4770, -77.6685), new GeoPoint(38.6316, -77.2620)))
  //   //println(countWithinRadius(new GeoPoint(38.4770, -77.6685), Array(new GeoPoint(38.6316, -77.2620)), 43.0))
  //   println(shortestDistance(new GeoPoint(38.4770, -77.6685), Array(new GeoPoint(38.6316, -77.2620))))
  // }

}