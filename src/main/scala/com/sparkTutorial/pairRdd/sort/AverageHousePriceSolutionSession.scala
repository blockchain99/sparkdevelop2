package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.spark.sql.SparkSession

object AverageHousePriceSolutionSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bathRoomPrice")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val houseRdd = spark.sparkContext.textFile("in/RealEstate.csv")
    val firstLine = houseRdd.first()
    val pureRdd = houseRdd.filter(line => line != firstLine)
    println("pureRdd.first() :" + pureRdd.first())
    /*(bedRoomNumber.toInt, (1. housePrice.toDouble)) : To sort by hedRoomNumber, it shoud be Int */
    val housePricePairRdd = pureRdd.map(
      line => (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))
    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))
    val resultRdd = housePriceTotal.mapValues(avgCount => avgCount.total/avgCount.count)
//    for((count, price) <- resultRdd) println(count + " : " + price)

    println("------------------------sortByKey(ascending = false)----------------------")
    val sortedRdd = resultRdd.sortByKey(ascending = false)
    for((count, price) <- sortedRdd) println(count + " : " + price)

  }

}
