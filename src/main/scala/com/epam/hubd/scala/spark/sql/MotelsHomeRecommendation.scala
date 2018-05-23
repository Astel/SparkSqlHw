package com.epam.hubd.scala.spark.sql

import com.epam.hubd.scala.spark.sql.domain.{BidItem, EnrichedItem}
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(
      args.length == 4,
      "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .enableHiveSupport
      .master("local[4]")
      .appName("motels-home-recommendation")
      .getOrCreate

    val sc = spark.sparkContext

    val sqlContext = spark.sqlContext

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: SQLContext,
                  bidsPath: String,
                  motelsPath: String,
                  exchangeRatesPath: String,
                  outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write.format("com.databricks.spark.csv").save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] =
      getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write.format("com.databricks.spark.csv").save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: SQLContext, bidsPath: String): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.textFile(bidsPath)
      .map(_.split(Constants.DELIMITER))
      .toDF()
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    println(rawBids)

    rawBids

//    rawBids.filter($"arr[2]")
//          .filter()
//      .filter(list => list(2).contains("ERROR"))
//      .map(str => BidError(str(1), str(2)))
//      .map(bidError => (bidError, 1))
//      .reduceByKey(_ + _)
//      .map(m => m._1.toString + "," + m._2)
  }

  def getExchangeRates(sqlContext: SQLContext,
                       exchangeRatesPath: String): Map[String, Double] = ???
//  {
//    import sqlContext.implicits._
//
//    sc.textFile(exchangeRatesPath)
//      .map(_.split("\n"))
//      .map(arr => arr.flatMap(_.split(Constants.DELIMITER)))
//      .map(arr => arr(0) -> arr(3).toDouble)
//      .collect()
//      .toMap[String, Double]
//  }

  def getBids(rawBids: DataFrame,
              exchangeRates: Map[String, Double]): DataFrame = ???
 /* {
    val exchangesDates = exchangeRates.map(m =>
      DateTime.parse(m._1, Constants.INPUT_DATE_FORMAT) -> m._2)

    rawBids
      .filter(list => !list(2).contains("ERROR") && list.size > 8)
      .flatMap(list => {
        val bidData = DateTime.parse(list(1), Constants.INPUT_DATE_FORMAT)
        val outputData = bidData.toString(Constants.OUTPUT_DATE_FORMAT)

//        val exchanges = exchangesDates
//          .filter(m => bidData.isAfter(m._1) || bidData.isEqual(m._1))
//          .reduceLeft((d1, d2) => if (d1._1.isAfter(d2._1) || d1._1.isEqual(d2._1)) d1 else d2)
//
//        if(list(0).contains("0000005") && list(1).contains("01-13-05-2016"))
//          println(exchanges._1.toString() + " " + exchanges._2 + " " + list(8) + " " + (exchanges._2 * list(8).toDouble).formatted("%.3f"))

        val exchangesRate = exchangesDates
          .filter(m => bidData.isAfter(m._1) || bidData.isEqual(m._1))
          .reduceLeft((d1, d2) => if (d1._1.isAfter(d2._1) || d1._1.isEqual(d2._1)) d1 else d2)._2

        if(list(5) == "" && list(6) == "" && list(8) == "") {
          List[BidItem]()
        }
        else {
          val bid = List(5,8,6).flatMap(num =>
            if (!list(num).equals("")) {
              List[BidItem](BidItem(list.head, outputData, BIDS_HEADER(num), list(num).toDouble * exchangesRate))
            } else {
              List[BidItem]()
            }
          ).maxBy(_.price)
          List(bid)
        }
      })
  }*/

  def getMotels(sqlContext: SQLContext, motelsPath: String): DataFrame = ???
//  {
//    sc.textFile(motelsPath)
//      .map(_.split("\n"))
//      .map(str => str.flatMap(_.split(Constants.DELIMITER)))
//      .map(arr => (arr(0), arr(1)))
//  }

  def getEnriched(bids: DataFrame,
                  motels: DataFrame): DataFrame = ???
//  {
//    val broadcastedMotels =
//      motels.map(item => item._1 -> item._2).collect().toMap
//
//    bids.map(item => {
//      val motelName = broadcastedMotels.getOrElse(item.motelId, "unknown")
//      EnrichedItem(item.motelId, motelName, item.bidDate, item.loSa, item.price)
//    })
//  }
}
