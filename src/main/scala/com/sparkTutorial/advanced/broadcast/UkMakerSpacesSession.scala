package com.sparkTutorial.advanced.broadcast


import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

import scala.io.Source


object UkMakerSpacesSession extends Serializable {
  /* Broadcast variables keep read-only variable cached on each machine rather than
      * shipping a copy of it with task. To give every node, a copy of a large input dataset,
        * in efficient way*. it will be kept at all the worker nodes for use in one or more
        * Spark operations. */
  /* How are those maker spaces distributed across different regins in the UK ?
  * but in uk-makerspace-identifiable-data.csv , there is no regins information except
  * postal code., so We need another csv file, uk-postcode.csv, which has Region information
   * So, combining these two csv files, we can find the answer .*/
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UkMakerSpace")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

/* load the postcode and broadcast it across the cluster  */
    val postCodeMap = spark.sparkContext.broadcast(loadPostCodeMap())

    val makerSpaceRdd = spark.sparkContext.textFile("in\\uk-makerspaces-identifiable-data.csv")
    /*load maker space dataset(uk-makerspace-identifiable-data.csv) and call map operation
      * on maker space RDD to look up the region using the postcode of the maker space. */

    println("---------filtering out head line and return postCode from makerspaces file using def getPostPrefix -----")
    val regions = makerSpaceRdd
                  .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")  //remove first line
                    .filter(line => getPostPrefix(line).isDefined)  //Some(postcode.split(" ")(0)
      /*If the already loaded map from uk-postcode.csv  exists(by getPostPrefix function),
      Get postcode in loaded rdd of postcode file. If not found, return "Unknown"*/
                     .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))
    println("**** region : count Map is as follows : ****")
    for((region, count) <- regions.countByValue()) println(region + " : "+ count)

    println("*********************test option1 : read postcode file directly : *********************")
    val makerRdd = spark.sparkContext.textFile("in\\uk-postcode.csv")
    val first =makerRdd.first()
    val interimMakerRdd = makerRdd.filter(line => line != first)
    val postCodeRddMap = interimMakerRdd   //map[String, String]
                   .map(line => {
                     val splits = line.split(Utils.COMMA_DELIMITER, -1)  //limit is -1
                     splits(0) -> splits(7)
                   }).collectAsMap()
    println("postCodeRddMap.toString().length: "+postCodeRddMap.toString().length )
    for((k,v) <- postCodeRddMap) println("*"+k + " : " +v)

    println("*********************test option2 : read postcode file using def loadPostCodeMap() ****************")
    val postCodeRddMap2 = loadPostCodeMap()   //map[String, String]
    /*Map[splits(0) -> splits(7)],so In splits(0)-postcode field, remove heading with "postcode" line.  */
    val finalPostCodeRddMap2 = postCodeRddMap2.filter(line => !line._1.contains("Postcode"))
    println("finalPostCodeRddMap2.toString().length: "+finalPostCodeRddMap2.toString().length )  // 61427
    for((k,v) <- finalPostCodeRddMap2) println("**"+k + " : " +v)
  }//end of def main

  def getPostPrefix(line:String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)  //postcode field is splits(4) in uk-makerspaces-identifiable-data.csv
    /* remove blank for postcode, so it returns first element of the postcode field split by blank. */
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }
  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in\\uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      /* Map of poscode->Region  */
      splits(0) -> splits(7)
    }).toMap
  }
}
