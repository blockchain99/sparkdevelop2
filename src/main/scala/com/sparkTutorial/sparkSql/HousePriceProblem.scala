package com.sparkTutorial.sparkSql

import org.apache.spark.sql.SparkSession


object HousePriceProblem {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           group by location, aggregate the average price per SQ Ft and sort by average price per SQ Ft.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

        +----------------+-----------------+
        |        Location| avg(Price SQ Ft)|
        +----------------+-----------------+
        |          Oceano|             95.0|
        |         Bradley|            206.0|
        | San Luis Obispo|            359.0|
        |      Santa Ynez|            491.4|
        |         Cayucos|            887.0|
        |................|.................|
        |................|.................|
        |................|.................|
        */

  val PRICE_SQFT = "Price SQ Ft"
  val LOCATION = "Location"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
            .appName("HouseLocationAvgPriceSQFT")
            .master("local[*]")
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val realEstateDF = spark.read
                              .option("header", "true")
                                .option("inferSchema", value = true)
                                .csv("in//RealEstate.csv")
    realEstateDF.printSchema()
    realEstateDF.show()

    println("****** First Grouping location , calculate average price/sqft then print Location-wise, sqft price: orderBy(avg(Price SQ Ft)) ")
    realEstateDF.select(LOCATION, PRICE_SQFT).groupBy(LOCATION).avg(PRICE_SQFT).orderBy("avg(Price SQ Ft)")show()  //ascending

    println("-------- Without .select(LOCATION, PRICE_SQFT)---same as above-------")
    realEstateDF.groupBy(LOCATION).avg(PRICE_SQFT).orderBy("avg(Price SQ Ft)")show()  //ascending

    println("====== without oderBy(avg(Price SQ Ft))")
    realEstateDF.select(LOCATION, PRICE_SQFT).groupBy(LOCATION).avg(PRICE_SQFT).show()
  }
}
