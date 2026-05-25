package com.aml.common.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactionSpec extends AnyFlatSpec with Matchers {

  "CanonicalTransaction" should "create a valid instance" in {
    val txn = CanonicalTransaction(
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
      metadata = Map.empty,
      rawPayload = "{}"
    )
    txn.txnId shouldBe "TXN-001"
    txn.txnType shouldBe TxnType.TRANSFER
    txn.amount shouldBe BigDecimal(5000.00)
  }

  it should "parse from JSON" in {
    val json = """{"txnId":"TXN-001","txnType":"TRANSFER","timestamp":1716662400000,"amount":5000.00,"currency":"USD","amountUsd":5000.00,"direction":"OUTBOUND","customerId":"CUST-001","counterpartyId":"CUST-002","channel":"ONLINE","countrySrc":"US","countryDst":"GB","metadata":{},"rawPayload":"{}"}"""
    val txn = CanonicalTransaction.fromJson(json)
    txn.txnId shouldBe "TXN-001"
    txn.txnType shouldBe TxnType.TRANSFER
  }
}
