package com.aml.streaming.layer1

import com.aml.common.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NormalizationJobSpec extends AnyFlatSpec with Matchers {

  "SchemaValidator" should "validate a correct transaction" in {
    val txn = CanonicalTransaction(
      txnId = "TXN-001", txnType = TxnType.TRANSFER, timestamp = 1716662400000L,
      amount = BigDecimal(5000), currency = "USD", amountUsd = BigDecimal(5000),
      direction = Direction.OUTBOUND, customerId = "C1", counterpartyId = "C2",
      channel = "ONLINE", countrySrc = "US", countryDst = "GB",
      metadata = Map.empty, rawPayload = "{}"
    )
    SchemaValidator.isValid(txn) shouldBe true
  }

  it should "reject transaction with empty txnId" in {
    val txn = CanonicalTransaction(
      txnId = "", txnType = TxnType.TRANSFER, timestamp = 1716662400000L,
      amount = BigDecimal(5000), currency = "USD", amountUsd = BigDecimal(5000),
      direction = Direction.OUTBOUND, customerId = "C1", counterpartyId = "C2",
      channel = "ONLINE", countrySrc = "US", countryDst = "GB",
      metadata = Map.empty, rawPayload = "{}"
    )
    SchemaValidator.isValid(txn) shouldBe false
  }

  it should "reject transaction with negative amount" in {
    val txn = CanonicalTransaction(
      txnId = "TXN-001", txnType = TxnType.TRANSFER, timestamp = 1716662400000L,
      amount = BigDecimal(-100), currency = "USD", amountUsd = BigDecimal(-100),
      direction = Direction.OUTBOUND, customerId = "C1", counterpartyId = "C2",
      channel = "ONLINE", countrySrc = "US", countryDst = "GB",
      metadata = Map.empty, rawPayload = "{}"
    )
    SchemaValidator.isValid(txn) shouldBe false
  }

  it should "reject transaction with invalid currency code" in {
    val txn = CanonicalTransaction(
      txnId = "TXN-001", txnType = TxnType.TRANSFER, timestamp = 1716662400000L,
      amount = BigDecimal(5000), currency = "INVALID", amountUsd = BigDecimal(5000),
      direction = Direction.OUTBOUND, customerId = "C1", counterpartyId = "C2",
      channel = "ONLINE", countrySrc = "US", countryDst = "GB",
      metadata = Map.empty, rawPayload = "{}"
    )
    SchemaValidator.isValid(txn) shouldBe false
  }
}
