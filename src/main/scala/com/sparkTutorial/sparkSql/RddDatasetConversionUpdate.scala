package com.sparkTutorial.sparkSql

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RddDatasetConversionUpdate {

  def toDouble(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
                     .setAppName("stackOverflowRddDsConv")
                     .setMaster("local[1]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("stackOverflowRddDsConv")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv") //RDD
    println("================ RDD head line==============")
    println(lines.first())
    val first = lines.first()
    val lineExFirst = lines.filter(line => line != first)
    println( "RDD take(2): ")
    lineExFirst.take(2).foreach(println)

    /* .equals("country") means it's head line */
    val responseRddConv = lines
      /* filter out the head line - */
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))  //remove heas line
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        StackOver(splits(2), toDouble(splits(6)), splits(9), toDouble(splits(14)))
    })

    import spark.implicits._
    val responseDS = lines.toDS()

    println("===================print out DS schema==================")
    responseDS.printSchema()

    println("---------- 5 records of response table------------")
    responseDS.show(5)

    for (response <-responseDS.rdd.collect()) println(response)

  }
}

/* splits(2),splits(6),splits(9),splits(14) */
case class StackOver(country: String, age_midpoint: Option[Double], occupation: String, salary_midpoint: Option[Double] )
