package com.sparkTutorial.advanced.accumulator

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object StackOverFlowSruveySession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("stackOverflowSurvey")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*spark.sparkContext.longAccumulator : aggregating information across the executors.
    Accumulators are write-only variable, task on worker nodes cannot access the accumulator's value  */
    val total = spark.sparkContext.longAccumulator
    val missingSalaryMidPoint = spark.sparkContext.longAccumulator

    val responseRDD = spark.sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")
    /*  column 14: salaryMidpoint, splits(14), column 2 : country, splits(2)  */
    val responseFromCanada = responseRDD.filter(response => {
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }

      splits(2) =="Canada"
    })

    println("Count of response from Canada : "+responseFromCanada.count())
    println("Total count of responses : "+ total.value)
    println("Count of respnses missing salary middle point : "+ missingSalaryMidPoint.value)


  }
}
