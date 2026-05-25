package com.aml.streaming.layer2

case class WindowStats(
  txnCount: Long,
  totalAmount: BigDecimal,
  maxAmount: BigDecimal,
  avgAmount: BigDecimal,
  exceedsCtrThreshold: Boolean
)

object WindowAggregator {
  def calculateStats(amounts: Seq[BigDecimal], ctrThreshold: BigDecimal): WindowStats = {
    if (amounts.isEmpty) {
      WindowStats(0, BigDecimal(0), BigDecimal(0), BigDecimal(0), false)
    } else {
      val total = amounts.reduce(_ + _)
      val max = amounts.max
      val avg = total / amounts.size
      WindowStats(
        txnCount = amounts.size,
        totalAmount = total,
        maxAmount = max,
        avgAmount = avg,
        exceedsCtrThreshold = amounts.exists(_ >= ctrThreshold)
      )
    }
  }
}
