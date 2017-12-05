package com.sparkTutorial.pairRdd.mapValues

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */
    val spark = SparkSession.builder()
      .appName("upperCase")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val airportsRDD = spark.sparkContext.textFile("in\\airports.text")
    //(airportName, country) pair
    val pairRdd = airportsRDD.map((line: String) => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)) )
    //    val pairRdd2 = airportsRDD.map(s => (s.split(" ")(1), s.split(" ")(3)) )  //(IndexOutOfBoundsException error !
    println(pairRdd.first())  //("Goroka","Papua New Guinea")
    /* Convert value to upperstring using mapValues while Key is same */
    /* key is not change but value change : RDD.mapValues(value => value.toUpperCase)  */
    val upperRdd = pairRdd.mapValues(countryName => countryName.toUpperCase())
    upperRdd.saveAsTextFile("out\\airports_uppercase.txt")
  }
}
