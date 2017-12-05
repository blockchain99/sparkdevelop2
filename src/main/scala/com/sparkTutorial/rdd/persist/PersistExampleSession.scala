package com.sparkTutorial.rdd.persist

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistExampleSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sumOfNumber")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    /* (1 to 40).toList - List[Int], List(1 to 40) - List[Range]             */
    //    val listRange = List(1 to 40)  //List[Range.inclusive]
    //    val listInt = (1 to 40)toList  //List[Int]
    val inputInt = spark.sparkContext.parallelize((1 to 40).toList)
    inputInt.persist(StorageLevel.MEMORY_ONLY)  //
    inputInt.reduce(_*_)
    println(inputInt.count())
  }
}
