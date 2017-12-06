package com.sparkTutorial.advanced.accumulator

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object StackOverFlowSurveyFollowUpSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("stackOverflowSurvey")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*Accumulators are write-only variable, taks on worker nodes cannot access the accumulator's value  */
    val total = spark.sparkContext.longAccumulator
    val missingSalaryMidPoint = spark.sparkContext.longAccumulator
    val processedBytes = spark.sparkContext.longAccumulator

    val responseRDD = spark.sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

    /*  column 14: salaryMidpoint, splits(14), column 2 : country, splits(2)  */
    val responseFromCanada = responseRDD.filter(response => {

      /* get the Bytes of line from each survey. */
      processedBytes.add(response.getBytes().length)
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }

      splits(2) =="Canada"
    })

    println("Count of response from Canada : "+responseFromCanada.count())
    println("Number of bytes processed : "+processedBytes)
    println("Total count of responses : "+ total.value)
    println("Count of respnses missing salary middle point : "+ missingSalaryMidPoint.value)
  }

}
