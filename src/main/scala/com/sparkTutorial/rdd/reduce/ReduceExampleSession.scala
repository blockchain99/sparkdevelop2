package com.sparkTutorial.rdd.reduce

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object ReduceExampleSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("reduceEx")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputInteger = (1 to 5).toList  //List[Int]
    val integerRdd = spark.sparkContext.parallelize(inputInteger)  //RDD[Int]

    val product = integerRdd.reduce((x,y) => x*y)
    println("product : "+product)
    println("*******************************")
    val inputString = List("This", "That", "Test", "This", "Exercise")
    val inputStringRdd = spark.sparkContext.parallelize(inputString)
    val result = inputStringRdd.flatMap(list => list.split(Utils.COMMA_DELIMITER))
   //     .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_+_)
    println("result : "+result)
    println(result.foreach(println))
    //.collect()  //convert RDD to Array

}

}
