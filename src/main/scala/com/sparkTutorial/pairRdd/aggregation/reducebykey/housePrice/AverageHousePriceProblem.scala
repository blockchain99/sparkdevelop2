package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object AverageHousePriceProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the house data from in/RealEstate.csv,
       output the average price for houses with different number of bedrooms.

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

       (3, 325000)  //(numberOfBedroom, avgPrice)
       (1, 266356)
       (2, 325000)
       ...

       3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
     */
    val spark = SparkSession.builder()
      .appName("bathRoomPrice")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val houseRdd = spark.sparkContext.textFile("in/RealEstate.csv")
    val firstLine = houseRdd.first()
    val pureRdd = houseRdd.filter(line => line != firstLine)
    println("pureRdd.first() :" + pureRdd.first())

    //    val pairRdd = pureRdd.map(line => (line.split(Utils.COMMA_DELIMITER)(3),
    //      line.split(Utils.COMMA_DELIMITER)(2)))
    println("================ option1 using groupByKey ==============")
    /*option1 Rdd.map(p => (p.makeKey, p.makeValue))
    using pariRdd.groupByKey() in which pairRdd consists of (key, value)*/
    val pairRdd = pureRdd.map(line => (line.split(Utils.COMMA_DELIMITER)(3),
      line.split(Utils.COMMA_DELIMITER)(2).toDouble))   //(bedRoomNumber, housePrice)
    println("** pairRdd.first() :" + pairRdd.first())
    //    val bedroomAvgPriceRdd = pairRdd.flatMap()
    println("-pairRdd.count() :" + pairRdd.count())
    /* making RDD with key, value pair is omitted   */
    val interimResult = pairRdd.groupByKey() //RDD[Key, Iterable[Value]
      .map(p => (p._1, (p._2.size, p._2.sum))) // key(bedroom), (total number for the bedroom, sum of the bedroom
    println("* interimResult.first(): ")
    println(interimResult.first())
    println("---------final result--------------------")
    val finalResult = pairRdd.groupByKey()
      .map(p => (p._1, p._2.sum / p._2.size))
     //   .collect()  //convert from RDD to Array
    finalResult.foreach(println)
    println("================ option2 using reducByKey ==============")
    /* optoin2 Rdd.map(p => (p.bedroomNumber, (1, p.price))
    using reduceByKey((v1,v2) => ((v1._1 + v2._1),(v1._2 + v2._2))  :
                             (v1._1 + v2._1): freuency of specific bedroom number:(1+1+1...)
                              (v1._2 + v2._2): sum of specific bedroom number*/
    val pairRdd2 = pureRdd.map(line => (line.split(Utils.COMMA_DELIMITER)(3),
      (1, line.split(Utils.COMMA_DELIMITER)(2).toDouble)))
    /* (bedroomNumber, (1, housrPrice))  : (key, (value._1, value._2))   */
    println(pairRdd2)
    /* reduceByKey is only for value */
    val interimRdd2 = pairRdd2.reduceByKey((v1, v2) =>( (v1._1 + v2._1), (v1._2 + v2._2) ) )
    println("interimRdd2.first() : "+ interimRdd2.first())
//    val finalRdd2 = pairRdd2.reduceByKey((v1, v2)=> ( (v1._2 + v2._2) / (v1._1+v2._1) ) )  //error !
    val finalRdd2 = interimRdd2.map(p => (p._1,(p._2._2/p._2._1)) )
    println(finalRdd2.first())
    println("*** ( bedromm number , average price)****")
    finalRdd2.foreach(println)

    /* using : case class AvgCount(count: Int, total: Double)*/
    val finalRdd2_1 = interimRdd2.mapValues(avgCount => avgCount._2/avgCount._1)
    println("Result using : mapValues(avgCount => avgCount._2/avgCount._1) : ")
    for((roomNumber, price) <- finalRdd2_1.collect) println(roomNumber + ":" + price)

    /**************using case class AvgCount (count, total)**********************/
      /* RDD[(Int, AvgCount)]  */
    val pairRdd2_AvgCount = pureRdd.map(line => (line.split(Utils.COMMA_DELIMITER)(3).toInt,
      AvgCount(1, line.split(Utils.COMMA_DELIMITER)(2).toDouble)))
    /*reduceByKey : only value */
    val interimRdd2AvgCount = pairRdd2_AvgCount.reduceByKey((v1, v2) =>AvgCount(v1.count + v2.count, v1.total + v2.total))
   /* mapValues : only value */
   /*  mapValues preserve key of each element in the previous RDD: recommand to use mapValues over map(change the key)  */
    val finalRdd2_AvgCount = interimRdd2AvgCount.mapValues(avgCount => avgCount.total/avgCount.count)
    for((roomN, price) <- finalRdd2_AvgCount) println(roomN + " : "+ price)
  }
}
