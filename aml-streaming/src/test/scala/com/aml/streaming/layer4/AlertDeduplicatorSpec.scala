package com.aml.streaming.layer4

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

class AlertDeduplicatorSpec extends AnyFlatSpec with Matchers {

  "AlertDeduplicator" should "deduplicate alerts with same rule and customer within window" in {
    val now = Instant.now()
    val alerts = List(
      AlertInfo("A1", "CUST-001", "SAR-001", "HIGH", now),
      AlertInfo("A2", "CUST-001", "SAR-001", "HIGH", now.plusSeconds(60)),
      AlertInfo("A3", "CUST-002", "SAR-001", "HIGH", now)
    )
    val deduplicated = AlertDeduplicator.deduplicate(alerts, windowMinutes = 30)
    deduplicated.size shouldBe 2 // A1+A2 merged, A3 separate
  }

  it should "keep alerts from different customers" in {
    val now = Instant.now()
    val alerts = List(
      AlertInfo("A1", "CUST-001", "SAR-001", "HIGH", now),
      AlertInfo("A2", "CUST-002", "SAR-001", "HIGH", now)
    )
    val deduplicated = AlertDeduplicator.deduplicate(alerts, windowMinutes = 30)
    deduplicated.size shouldBe 2
  }

  it should "prioritize by severity" in {
    val now = Instant.now()
    val alerts = List(
      AlertInfo("A1", "CUST-001", "SAR-001", "MEDIUM", now),
      AlertInfo("A2", "CUST-001", "SAR-001", "CRITICAL", now.plusSeconds(10))
    )
    val deduplicated = AlertDeduplicator.deduplicate(alerts, windowMinutes = 30)
    deduplicated.size shouldBe 1
    deduplicated.head.severity shouldBe "CRITICAL"
  }
}
