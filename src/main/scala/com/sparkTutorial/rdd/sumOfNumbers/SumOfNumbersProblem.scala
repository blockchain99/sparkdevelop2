package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.sql.SparkSession

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    val spark = SparkSession.builder()
      .appName("sumOfNumber")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /****** option2 using prime_nums.text : read first 100 primes,then sum of them ******/
    val readPrimeRdd = spark.sparkContext.textFile("in\\prime_nums.text")
    /*\\s same [\\t\\n\\x0B\\f\\r]
    * and readPrimeRdd : RDD[String], so need to be Int by map*/
    val sumOfPrime = readPrimeRdd.flatMap(line => line.split("\\s+"))  //"\\s+" empty space between [space] and [tab]
                    .filter(number => !number.isEmpty)
                    .map(number => number.toInt)
                      .reduce(_+_)
     println("--------------sumOfPrime : "+sumOfPrime)

/* Solutoin2 : using Prime number generated from 2 to 100  */
    val listRange = List(2 to 100)  //List[Range]
    val listInt = (2 to 100).toList  //List[Int]
    val listIntRdd = spark.sparkContext.parallelize(listInt)
    val numberL = listIntRdd.filter(x =>isPrime(x))

    val listFilter = listInt.filter(x => isPrime(x))  //it works: List[Int]
    println("* listFilter :"+listFilter)
    //listRange.filter(x=>isPrime(x))  //error!  since listRnage is List[Range]
    val sumList = listFilter.reduce(_+_)  //as for List
    val sumList2 = listFilter.reduce((x,y) => x+y)   //same as listFilter.sum
    println("* sumList :" + sumList+" * List.sum : "+listFilter.sum)
    println("* sumList2 :" + sumList2)


    /************ following line : not comply with the question, only for reviewing purpose ***************/
    val take100 = readPrimeRdd.take(100) //Array(100), object
    val take100List = take100.toList  //List
    println("**take100List:"+take100List)
    println()
    val sumOfFirst100Prime = take100List.reduceLeft(_+_)  //Array.reduceLeft, not working as intented ! warning
    println("sumOfFirst100Prime :"+sumOfFirst100Prime)
    val sumOfFirst100Prime2 = take100List.reduce((x,y) => x+y)    //Array.reduce, not working as intented ! warning
    println("sumOfFirst100Prime2 :"+sumOfFirst100Prime2)
//    val sumOfFirst100Prime2 = readPrimeRdd.take(100).reduce((_+_))    //Array.reduce , same as above

  }//end of main

  def isPrime(n: Int):Boolean ={
    (2 until n) forall(x => n%x != 0)
  }
}
