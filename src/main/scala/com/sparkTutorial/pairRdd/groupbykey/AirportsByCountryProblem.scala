package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports(value) located in each country(key).

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */
    val spark = SparkSession.builder()
      .appName("upperCase")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val airportsRDD = spark.sparkContext.textFile("in\\airports.text")  //head column without feature name
    //(country, airportName ) pair
    val pairRdd = airportsRDD.map((line: String) => (line.split(Utils.COMMA_DELIMITER)(3), line.split(Utils.COMMA_DELIMITER)(1)) )
    val airportsByCountry = pairRdd.groupByKey()
    airportsByCountry.first()
//    for((country, airport) <- airportsByCountry.collect()) println(country +" : "+airport) //CompactBuffer(airport list)
    for((country, airport) <- airportsByCountry.collectAsMap()) println(country +" : "+airport.toList) //List output for airportName
  }
}
