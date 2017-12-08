package com.sparkTutorial.sparkSql

import org.apache.spark.sql.SparkSession

object TypedDatasetUpdate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("stackOverflowTypedDataSet")
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

    val responseWithSelectedItem = responseDFCSV.select("country", "age_midpoint", "occupation", "salary_midpoint")
   /* Spark map each row to Response object, To pass Response object across a different node in the cluster,
    *Encoder tell Spark to generate code at run time to serialize Response object to binary structure to be efficient
    * data processing.*/
    /* Type is Response :
     * case class Response(country: String, age_midpoint: Option[Double], occupation: String, salary_midpoint: Option[Double]) */
    /* .as method : return a new DataSet for selected DataSet mapped to a specified data type*/
    import spark.implicits._
    val typedDataSet = responseWithSelectedItem.as[Response]

    println("==============print out Schema============")
    typedDataSet.printSchema()

    /* With typedDS, it is much easier to write filter condition */
    println("-----------------Selected Item of table------------------")
    typedDataSet.show(5)     //top 20 show

    println("********Print records where the response is from Albania")
    println("-----------------DF.filter(DF.col(\"country\") === \"Albania\")------------------")
    responseWithSelectedItem.filter(responseWithSelectedItem.col("country")==="Albania").show()
    typedDataSet.filter(response => response.country == "Albania").show(5)

    println("********print the count of occupation")
    responseWithSelectedItem.groupBy("occupation").count().show()
    typedDataSet.groupBy("occupation").count().show(5)
    typedDataSet.groupBy(typedDataSet.col("occupation")).count().show(5)

    println("********Print records with average mid age less than 20")
    println("-----------------DF.filter(DF.col(\"age_midpoint\") < 20 )------------------")
    responseWithSelectedItem.filter(responseWithSelectedItem.col("age_midpoint") < 20).show(5)
    typedDataSet.filter(response => response.age_midpoint.isDefined && response.age_midpoint.get < 20).show(5)

    println("*******Print the result by salary middle point in descending order: DF.sort(DF.col(\"colName\").desc)***")
    responseWithSelectedItem.sort(responseWithSelectedItem.col("salary_midpoint").desc).show(5)
    typedDataSet.sort(typedDataSet.col("salary_midpoint").desc).show(5)

    println("*******Print the result by salary middle point in descending order: DF.orderBy(DF.col(\"colName\").desc)***")
    responseWithSelectedItem.orderBy(responseWithSelectedItem.col("salary_midpoint").desc).show(5)
    typedDataSet.orderBy(typedDataSet.col("salary_midpoint").desc).show(5)

    println("*******Group by country and aggregated by max,(min,average) salary middle point *****")
    responseWithSelectedItem.groupBy("country").max("salary_midpoint").show(5)  //.max , .min , .avg
    typedDataSet.filter(response => response.salary_midpoint.isDefined).groupBy("country").max("salary_midpoint").show(5)  //.max , .min , .avg
    //typedDataSet.filter(response => response.salary_midpoint.isDefined).groupBy("country").min("salary_midpoint").show()  // .min
    //typedDataSet.filter(response => response.salary_midpoint.isDefined).groupBy("country").avg("salary_midpoint").show()  // .avg

    println("============= Make new \"salary midpoint bucket\" column ================= " )
    println( "DF.withColumn(\"newColumnName\",\"theway_how_to_make_newColumn\")"  )
    val responseWithSalaryBucket = responseDFCSV.withColumn("salary_midpoint_bucket",
      responseDFCSV.col("salary_midpoint").divide(2000).cast("integer").multiply(2000))

    println("********** With salary bucket column : DF.select(\"colName\")*****")
    responseWithSalaryBucket.select("salary_midpoint", "salary_midpoint_bucket").show()

    println("********* Group by salary bucket and it's count, orderBy it ")
    responseWithSalaryBucket.groupBy("salary_midpoint_bucket").count().orderBy("salary_midpoint_bucket").show()  //orderBy : default is ascending

    println("********* sortBy with .desc**************")
    responseWithSalaryBucket.groupBy("salary_midpoint_bucket").count().orderBy(responseWithSalaryBucket.col("salary_midpoint_bucket").desc).show()

    println("============= typedDataSet : Make new \"salary midpoint bucket\" column==============")
    typedDataSet.map(response => response.salary_midpoint.map(point => Math.round(point / 20000) * 20000).orElse(None) )
                    .withColumnRenamed("value", "salary_midpoint_bucket")
                    .groupBy("salary_midpoint_bucket")
                    .count()
                    .orderBy("salary_midpoint_bucket").show(5)
  }
}
