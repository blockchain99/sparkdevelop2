package com.sparkTutorial.rdd.reduce

import org.apache.spark.sql.SparkSession

object ReduceExampleSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("reduceEx")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputInteger = (1 to 5).toList
    val integerRdd = spark.sparkContext.parallelize(inputInteger)

    val product = integerRdd.reduce((x,y) => x*y)
    println("product : "+product)
  }

}
