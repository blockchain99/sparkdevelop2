package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID(0), Name of airport(1), Main city served by airport(2), Country where airport is located(3), IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    val conf = new SparkConf().setAppName("airport").setMaster("local[2]")  //2core, local[*]: all available core
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR") //Remove INFO message during runtime
    val airports = sc.textFile("in\\airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")
//    val airportsInUSA2 = airports.filter(line => line.split(" ")(3) == "\"United States\"")

    val airportNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + " , " + splits(2)
    })
/*** test : RDD to DF conversion ****/
    println("RDD count : "+airportNameAndCityNames.count())
    println( "RDD take(2): "+airportNameAndCityNames.take(2).foreach(println))
    /*convert RDD to DataFrame  */
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val airportNameAndCityNamesDF = airportNameAndCityNames.toDF()

    airportNameAndCityNamesDF.printSchema()  //Schema of DataFrame
    airportNameAndCityNamesDF.show(5)  //DataFrame.show()
/******* end of test *********/

    airportNameAndCityNames.saveAsTextFile("out\\airports_in_usa_update.text")
//    val airportNameAndCityNames2 = airportsInUSA.map(line =>
//      line.split(Utils.COMMA_DELIMITER)(1) + line.split(Utils.COMMA_DELIMITER)(2)
//    )
//    airportNameAndCityNames2.saveAsTextFile("out\\airports_in_usa_update2.text ")
  }
}
