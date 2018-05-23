package com.epam.hubd.scala.spark.sql.operation

import java.io.File
import java.nio.file.Files

import com.epam.hubd.scala.spark.sql.MotelsHomeRecommendation._
import com.epam.hubd.scala.spark.sql.MotelsHomeRecommendation
import com.epam.hubd.scala.spark.sql.operation.util.RddComparator
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.junit.Assert
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with DataFrameSuiteBase {
  System.setProperty("hadoop.home.dir", "c:\\")

  import sqlContext.implicits._

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    ).toDF

    val rawBids = MotelsHomeRecommendation.getRawBids(sqlContext, INPUT_BIDS_SAMPLE)

    assertDataFrameEquals(expected, rawBids)
  }

  test("should read raw motels") {
    val expected = sc.parallelize(
      Array(
        ("0000007", "Big River Copacabana Inn"),
        ("0000001", "Olinda Windsor Inn"),
        ("0000008", "Sheraton Moos' Motor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000009", "Moon Light Sun Sine Inn"),
        ("0000010", "Copacabana Motor Inn"),
        ("0000003", "Olinda Big River Casino"),
        ("0000004", "Majestic Big River Elegance Plaza"),
        ("0000005", "Majestic Ibiza Por Hostel"),
        ("0000006", "Mengo Elegance River Side Hotel")
      )
    ).toDF

    val rawBids = MotelsHomeRecommendation.getMotels(sqlContext, INPUT_MOTELS_INTEGRATION)

    assertDataFrameEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    ).toDF

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    ).toDF

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertDataFrameEquals(expected, erroneousRecords)
  }


  test("should filter errors and create correct aggregates") {
    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertDFTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertDFTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath).toDF()
    val actual = sc.textFile(actualPath).toDF()
    assertDataFrameEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
