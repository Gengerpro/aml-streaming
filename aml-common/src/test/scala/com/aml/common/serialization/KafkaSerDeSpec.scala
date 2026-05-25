package com.aml.common.serialization

import com.aml.common.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaSerDeSpec extends AnyFlatSpec with Matchers {

  val sampleTxn = CanonicalTransaction(
    txnId = "TXN-001",
    txnType = TxnType.TRANSFER,
    timestamp = 1716662400000L,
    amount = BigDecimal(5000.00),
    currency = "USD",
    amountUsd = BigDecimal(5000.00),
    direction = Direction.OUTBOUND,
    customerId = "CUST-001",
    counterpartyId = "CUST-002",
    channel = "ONLINE",
    countrySrc = "US",
    countryDst = "GB",
    metadata = Map("source_system" -> "core_banking"),
    rawPayload = """{"original":"data"}"""
  )

  "TransactionSerDe" should "serialize and deserialize correctly" in {
    val bytes = TransactionSerDe.serialize(sampleTxn)
    val decoded = TransactionSerDe.deserialize(bytes)
    decoded.txnId shouldBe sampleTxn.txnId
    decoded.txnType shouldBe sampleTxn.txnType
    decoded.amount shouldBe sampleTxn.amount
  }

  "AlertSerDe" should "serialize and deserialize alerts" in {
    val alert = Alert(
      alertId = "ALERT-001",
      txnId = "TXN-001",
      customerId = "CUST-001",
      alertType = AlertType.SAR,
      severity = Severity.HIGH,
      ruleId = "SAR-001",
      ruleDesc = "Structuring detected",
      score = 0.85f,
      status = AlertStatus.NEW,
      createdAt = java.time.Instant.now(),
      updatedAt = java.time.Instant.now()
    )
    val bytes = AlertSerDe.serialize(alert)
    val decoded = AlertSerDe.deserialize(bytes)
    decoded.alertId shouldBe alert.alertId
    decoded.alertType shouldBe AlertType.SAR
  }
}
