package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) println(word + " : " + count)
    println("20171130 version-for branch")






    /*same as above   */
//    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
//    counts.saveAsTextFile("C:\\Users\\Gloria\\scala-spark-tutorial\\word_count_sample.txt")
  }
}