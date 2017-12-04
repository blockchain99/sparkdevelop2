package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.sql.SparkSession

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    val spark =SparkSession.builder()
      .appName("hostAccessBoth")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val logJuly = spark.sparkContext.textFile("in\\nasa_19950701.tsv")
    val logAug = spark.sparkContext.textFile("in\\nasa_19950801.tsv")
    /*host both accessed by logJuly and logAug  */
    /*  test */
    /*july log with only host field  */
    val hostJuly = logJuly.map(line => line.split("\t")(0))
    /*Aug log with only host field  */
    val hostAug = logAug.map(line => line.split("\t")(0))

    val hostIntersection = hostJuly.intersection(hostAug)
    println("** count : "+ hostIntersection.count())  //38
    /*  firs line : "host" is included , so remove line with host */
    println("* hostIntersection.foreach(println) : "+hostIntersection.foreach(println))
   val hostIntersectionCleaned = hostIntersection.filter(line => line != "host")
    println("* hostIntersectionCleaned.count(): "+ hostIntersectionCleaned.count())  //37
    hostIntersectionCleaned.saveAsTextFile("out\\nasa_logs_same_hosts.csv")

  }
}
