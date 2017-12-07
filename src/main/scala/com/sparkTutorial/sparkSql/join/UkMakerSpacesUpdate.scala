package com.sparkTutorial.sparkSql.join

import org.apache.spark.sql.{SparkSession, functions}

object UkMakerSpacesUpdate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UkMakerSpaces_join")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*In makeSpaceDF(4), PostCode column: "SE15 3SN"  */
    val makeSpaceDF = spark.read   //sql.DataFrame
      .option("header", "true")
//      .option("inferSchema", value= true)
      .csv("in/uk-makerspaces-identifiable-data.csv")
    println("-----------------makeSpaceDF table------------------")
//    makeSpaceDF.printSchema()
    makeSpaceDF.show(5)

    val postCodeDF = spark.read
      .option("header", "true")
      //      .option("inferSchema", value= true)
      .csv("in/uk-postcode.csv")
      .withColumn("PostCode", functions.concat_ws("", functions.col("PostCode"), functions.lit(" ")))//make new column
    println("================= postCodeDF table======================")
//    postCodeDF.printSchema()
    postCodeDF.show(5)

    val postCodeDFOriginal = spark.read.option("header", "true").csv("in/uk-postcode.csv")
    println("*************** postCodeDFOriginal table***************")
    postCodeDFOriginal.show(5)

    val joinedDF = makeSpaceDF.join(postCodeDF, makeSpaceDF.col("PostCode").startsWith(postCodeDF.col("PostCode")), "left_outer")

    println("********* group by Region ************")
    println("================ joinedDF.grouBy(\"Region\") and count it's frequency with Descending order ============")
//    joinedDF.groupBy("Region").count().sort(joinedDF.col("count").desc).show(5)  //error : not resolve .col("count")

    joinedDF.groupBy("Region").count().orderBy("count").show(200)  //default: ascending order

    println("++++++++Test : orderBy Descding order+++++++++++++++++++++")
    val joinedDFGroupByRegionCount = joinedDF.groupBy("Region").count()
    joinedDFGroupByRegionCount.orderBy(joinedDFGroupByRegionCount.col("count").desc).show(200)

  }
}
