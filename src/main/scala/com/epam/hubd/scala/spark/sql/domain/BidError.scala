package com.epam.hubd.scala.spark.sql.domain

case class BidError(date: String, errorMessage: String) {

  override def toString: String = s"$date,$errorMessage"
}
