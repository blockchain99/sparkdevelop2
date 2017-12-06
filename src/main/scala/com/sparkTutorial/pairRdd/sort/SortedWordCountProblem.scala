package com.sparkTutorial.pairRdd.sort

import org.apache.spark.sql.SparkSession


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder
        .master("local") //org.apache.spark.SparkException: Could not parse Master URL: 'local(2)'
        .appName("wordCountSession")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val lines = spark.sparkContext.textFile("in/word_count.text") //""in\\word_count.text" : same result !
      /* option1 using flatMap & map & reduceByKey */
      /*wordCount : type is RDD[(String, Int)]  */
      val wordCount = lines.flatMap(line => line.split(" "))
        .map(word => (word,1))
        .reduceByKey(_+_)
      println("wordCount : "+ wordCount)
      for((word, count)<- wordCount) println("*"+word+"::"+count)

      println("-------------- descending order by countNumber using sortBy(_._2,... )--------------")
      val sortedRdd = wordCount.sortBy(_._2, ascending = false)
      for((word, count)<- sortedRdd) println("**"+word+"::"+count)

      println("-------------- descending order by countNumber using sortByKey()--------------")
      val sortedRdd2 = wordCount.map(x => (x._2, x._1)) // swap (key, value), now (count, word)
      val InterimSortedRdd2 = sortedRdd2.sortByKey(ascending = false)
      val finalsortedRdd2 = InterimSortedRdd2.map(x => (x._2, x._1))
      for((word, count)<- finalsortedRdd2) println("***"+word+"::"+count)



    }
}

