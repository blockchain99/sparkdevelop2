package com.sparkTutorial.sparkSql

import org.apache.spark.sql.SparkSession

/* Instead of typing column name with " " in select, filter clause,
    There is option to assign "columnName" to other variable name such as
     val SALARY_MIDPOINT = "salary_midpoint" ...*/

object StackOverFlowSurveyUpdated {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("stackOverflowSurveyDS_DF")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val responseRDD = spark.sparkContext.textFile("in\\2016-stack-overflow-survey-responses.csv")//RDD
//    val responseDF = spark.read.text("in\\2016-stack-overflow-survey-responses.csv")  //sql.DataFrame
//    responseDF.printSchema()

//    val responseDS = spark.read.textFile("in\\2016-stack-overflow-survey-responses.csv")  //sql.DataSet
//    println("-----------------DataSet schema------------------")
//    responseDS.printSchema()

    val responseDFCSV = spark.read   //sql.DataFrame
                          .option("header", "true")
                          .option("inferSchema", value= true)
                          .csv("in\\2016-stack-overflow-survey-responses.csv")  //or jdbc, json,...
    println("-----------------DataFrame schema------------------")
    responseDFCSV.printSchema()

    val responseWithSelectedItem = responseDFCSV.select("country", "age_midpoint", "gender", "occupation", "salary_midpoint")
    println("-----------------Selected Item of table------------------")
    responseWithSelectedItem.show()     //top 20 show

    println("********Print records where the response is from Albania")
    println("-----------------DF.filter(DF.col(\"country\") === \"Albania\")------------------")
    responseWithSelectedItem.filter(responseWithSelectedItem.col("country")==="Albania").show()

    println("********print the count of occupation")
    responseWithSelectedItem.groupBy("occupation").count().show()

    println("********Print records with average mid age less than 20")
    println("-----------------DF.filter(DF.col(\"age_midpoint\") < 20 )------------------")
    responseWithSelectedItem.filter(responseWithSelectedItem.col("age_midpoint") < 20).show()

    println("*******Print the result by salary middle point in descending order: DF.sort(DF.col(\"colName\").desc)***")
    responseWithSelectedItem.sort(responseWithSelectedItem.col("salary_midpoint").desc).show()

    println("*******Print the result by salary middle point in descending order: DF.orderBy(DF.col(\"colName\").desc)***")
    responseWithSelectedItem.orderBy(responseWithSelectedItem.col("salary_midpoint").desc).show()

    println("*******Group by country and aggregated by max,(min,average) salary middle point *****")
    responseWithSelectedItem.groupBy("country").max("salary_midpoint").show()  //.max , .min , .avg
//    responseWithSelectedItem.groupBy("country").min("salary_midpoint").show()  // .min
//    responseWithSelectedItem.groupBy("country").avg("salary_midpoint").show()  // .avg

    println("************ Make new \"salary midpoint bucket\" column *********** " )
    println( "DF.withColumn(\"newColumnName\",\"theway_how_to_make_newColumn\")"  )
    val responseWithSalaryBucket = responseDFCSV.withColumn("salary_midpoint_bucket",
      responseDFCSV.col("salary_midpoint").divide(2000).cast("integer").multiply(2000))

    println("********** With salary bucket column : DF.select(\"colName\")*****")
    responseWithSalaryBucket.select("salary_midpoint", "salary_midpoint_bucket").show()

    println("********* Group by salary bucket and it's count, orderBy it ")
    responseWithSalaryBucket.groupBy("salary_midpoint_bucket").count().orderBy("salary_midpoint_bucket").show()  //orderBy : default is ascending

    println("********* sortBy with .desc**************")
    responseWithSalaryBucket.groupBy("salary_midpoint_bucket").count().orderBy(responseWithSalaryBucket.col("salary_midpoint_bucket").desc).show()


  }

}
