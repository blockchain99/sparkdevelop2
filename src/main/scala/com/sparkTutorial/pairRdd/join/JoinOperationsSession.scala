package com.sparkTutorial.pairRdd.join

import org.apache.spark.sql.SparkSession

object JoinOperationsSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bathRoomPrice")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    /*pairRdd with name as key, age as value */
    val ages = spark.sparkContext.parallelize(List(("Tom", 29),("John", 22)))
    /* pairRdd with name as key, address as value  */
    val addresses = spark.sparkContext.parallelize(List(("James", "USA"), ("John", "UK")))
    val join = ages.join(addresses) //(John,(22,UK))
    join.saveAsTextFile("out/age_address_join.txt")

    val leftOuterJoin = ages.leftOuterJoin(addresses) //(Tom,(29,None)) (John,(22,Some(UK)))
    leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.txt")

    val rightOuterJoin = ages.rightOuterJoin(addresses)   //(James,(None,USA)) (John,(Some(22),UK))
    rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.txt")

    val fullOuterJoin = ages.fullOuterJoin(addresses)  //(Tom,(Some(29),None))(James,(None,Some(USA)))(John,(Some(22),Some(UK)))
    fullOuterJoin.saveAsTextFile("out/age_address_full_out_join.txt")

  }

}
