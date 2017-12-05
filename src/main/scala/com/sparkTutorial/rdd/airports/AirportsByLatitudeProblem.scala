package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession


object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       // airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)
       Then output the airport's name(1) and the airport's latitude(6) to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID(0), Name of airport(1), Main city served by airport(2), Country where airport is located(3), IATA/FAA code(4),
       ICAO Code(5), Latitude(6), Longitude(7), Altitude(8), Timezone(9), DST(10), Timezone in Olson format(11)

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
    val spark = SparkSession.builder
      .master("local")   //org.apache.spark.SparkException: Could not parse Master URL: 'local(2)'
      .appName("wordcound")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")// remove INFO message during run-time
    val airportsRDD = spark.sparkContext.textFile("in\\airports.text")
    /*find all the airports whose latitude are bigger than 40 */
    println(airportsRDD.first())
    val latitude_bg_40 = airportsRDD.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)
    val latitude_bg_40First=latitude_bg_40.first()  //String, first element in RDD
    println("* latitude_bg_40First : "+latitude_bg_40First)
//    out/airports_by_latitude.text
    /* airport's name and the airport's latitude of latitude_bg_40 */
    val nameLatitude = latitude_bg_40.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1)+","+splits(6)
    })
//    val nameLatitude2 = latitude_bg_40.map(line => line.split(" ")(1) +" , "+ line.split(" ")(6)) //same as above

    nameLatitude.saveAsTextFile("out\\name_latitude.txt")  //~.text error , so changed to ~.txt in windows.
  }
}
