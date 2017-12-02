package com.sparkTutorial.rdd.take

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    println("counts:" + counts)
    println(counts.count())
    println(counts.collect())
    println("*counts.foreach(println): ")
    counts.foreach(println)
    println("**************************************");println()
    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) println(word + ":" +count)
  }
}
