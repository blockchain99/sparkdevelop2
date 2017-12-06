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

/*reduceByKey : preferred , which save shuffle across network.  */
//  val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()  //manipulate values only
  val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey(_+_).collect()  //manipulate values only
  println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

  /*  mapValues preserve key of each element in the previous RDD: recommand to use mapValues over map(change the key)  */
  val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
  println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)

}
