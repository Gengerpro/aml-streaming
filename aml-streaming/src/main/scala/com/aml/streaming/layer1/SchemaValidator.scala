package com.aml.streaming.layer1

import com.aml.common.model._

object SchemaValidator {
  private val validCurrencyCodes = Set("USD", "EUR", "GBP", "JPY", "CNY", "CHF", "AUD", "CAD",
    "HKD", "SGD", "SEK", "NOK", "DKK", "NZD", "KRW", "INR", "BRL", "RUB", "ZAR", "MXN",
    "TRY", "THB", "MYR", "IDR", "PHP", "PLN", "CZK", "HUF", "RON", "BGN")

  def isValid(txn: CanonicalTransaction): Boolean = {
    txn.txnId.nonEmpty &&
    txn.amount > 0 &&
    txn.customerId.nonEmpty &&
    txn.counterpartyId.nonEmpty &&
    txn.timestamp > 0 &&
    validCurrencyCodes.contains(txn.currency.toUpperCase)
  }
}
