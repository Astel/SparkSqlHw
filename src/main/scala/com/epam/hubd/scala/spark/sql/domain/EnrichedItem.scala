package com.epam.hubd.scala.spark.sql.domain

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) {

  val formattedPrice = BigDecimal(price).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  override def toString: String = s"$motelId,$motelName,$bidDate,$loSa,$formattedPrice"
}
