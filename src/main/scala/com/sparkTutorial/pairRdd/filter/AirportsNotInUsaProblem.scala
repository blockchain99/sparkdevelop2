package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {
  /* (airPortName(1), countryName(3)) except in USA  */
    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport(1), Main city served by airport, Country where airport is located(3),
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    val spark = SparkSession.builder()
      .appName("reduceEx")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val airportsRDD = spark.sparkContext.textFile("in\\airports.text")
    val pairRdd = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)) )  //(airportName, country) pair
//    val pairRdd2 = airportsRDD.map(s => (s.split(" ")(1), s.split(" ")(3)) )  //(IndexOutOfBoundsException error !
    println(pairRdd.first())  //("Goroka","Papua New Guinea")
//    println(pairRdd2.first())

  }
}
