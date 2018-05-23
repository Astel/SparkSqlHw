package com.epam.hubd.scala.spark.sql.domain

case class BidRaw(motelId: String, bidDate: String, price: Double, prices: Map[String, Double]) {

}
