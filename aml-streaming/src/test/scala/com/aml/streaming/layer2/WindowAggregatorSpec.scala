package com.aml.streaming.layer2

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WindowAggregatorSpec extends AnyFlatSpec with Matchers {

  "WindowAggregator" should "calculate correct window stats" in {
    val amounts = List(
      BigDecimal(1000), BigDecimal(2000), BigDecimal(3000),
      BigDecimal(500), BigDecimal(1500)
    )
    val stats = WindowAggregator.calculateStats(amounts, BigDecimal(10000))
    stats.txnCount shouldBe 5
    stats.totalAmount shouldBe BigDecimal(8000)
    stats.maxAmount shouldBe BigDecimal(3000)
    stats.avgAmount shouldBe BigDecimal(1600)
    stats.exceedsCtrThreshold shouldBe false
  }

  it should "detect CTR threshold exceeded" in {
    val amounts = List(BigDecimal(15000))
    val stats = WindowAggregator.calculateStats(amounts, BigDecimal(10000))
    stats.exceedsCtrThreshold shouldBe true
  }
}
