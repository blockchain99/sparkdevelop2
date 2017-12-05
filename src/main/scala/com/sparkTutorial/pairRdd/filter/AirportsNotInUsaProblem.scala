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
      .appName("notInUSA")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val airportsRDD = spark.sparkContext.textFile("in\\airports.text")

    /*1,"Goroka","Goroka","Papua New Guinea","GKA","AYGA",-6.081689,145.391881,5282,10,"U","Pacific/Port_Moresby"
     * : elements are divided by "," not " " */
    //(airportName, country) pair
    val pairRdd = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)) )
//    val pairRdd = airportsRDD.map((line: String) => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)) )
    //    val pairRdd2 = airportsRDD.map(s => (s.split(" ")(1), s.split(" ")(3)) )  //(IndexOutOfBoundsException error !
    println(pairRdd.first())  //("Goroka","Papua New Guinea")
    /* remove airport in USA using KeyValue */
    val airPortNotInUSA = pairRdd.filter(KeyValue => KeyValue != "\"United States\"")
    airPortNotInUSA.saveAsTextFile("out\\airPortNotInUSA.txt")



  }
}
