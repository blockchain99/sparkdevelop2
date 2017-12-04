package com.sparkTutorial.rdd.collect

import org.apache.spark.sql.SparkSession

object CollectExampleSession {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("collectionSession")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
//    spark.sparkContext.textFile()
    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRDD = spark.sparkContext.parallelize(inputWords)
    println("* count word" + wordRDD.count())
    val wordCountByValue = wordRDD.countByValue()
    println("* CountByValue : ")
    for((word, count) <- wordCountByValue) println(word+" : "+count)
    /* option2 */
    val wordCountByValue2 = wordRDD
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .collect().toList
    println("**** option 2: wordCount *******")
    println(wordCountByValue2)
    /*  option 2 : count word */
    val lines = Seq("This is the first line",
                     "That is the second line",
                     "Total lines are three.")
    val wordLinesRdd = spark.sparkContext.parallelize(lines)
    val wordcount = wordLinesRdd
      .flatMap(line => line.split(" "))
        .map(word => (word, 1))
         .reduceByKey(_+_)
      .collect().toList
  println("--------------- wordcount : ")
    println(wordcount)
    /* collect : filtered RDD down to small size and deal it locally  */
    val words = wordRDD.collect()
  }

}
