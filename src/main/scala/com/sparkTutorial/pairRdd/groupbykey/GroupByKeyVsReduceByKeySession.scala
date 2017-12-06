package com.sparkTutorial.pairRdd.groupbykey

import org.apache.spark.sql.SparkSession

object GroupByKeyVsReduceByKeySession {
  val spark = SparkSession.builder()
    .appName("upperCase")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val words = List("one", "two", "two", "three", "three", "three")
  val wordsPairRdd = spark.sparkContext.parallelize(words).map(word => (word, 1))

  val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
  println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

  val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
  println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)

}
