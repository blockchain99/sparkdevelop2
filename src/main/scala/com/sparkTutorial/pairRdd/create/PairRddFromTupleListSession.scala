package com.sparkTutorial.pairRdd.create

import org.apache.spark.sql.SparkSession

object PairRddFromTupleListSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("reduceEx")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRddTuple = spark.sparkContext.parallelize(tuple)
    pairRddTuple.coalesce(1).saveAsTextFile("out\\pair_rdd_from_tuple_list")
  }
}
