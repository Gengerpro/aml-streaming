package com.aml.common.model

object TxnType extends Enumeration {
  type TxnType = Value
  val TRANSFER, CARD, FOREX = Value
}

object Direction extends Enumeration {
  type Direction = Value
  val INBOUND, OUTBOUND = Value
}

object AlertType extends Enumeration {
  type AlertType = Value
  val CTR, SAR, SANCTION, RULE = Value
}

object Severity extends Enumeration {
  type Severity = Value
  val LOW, MEDIUM, HIGH, CRITICAL = Value
}

object AlertStatus extends Enumeration {
  type AlertStatus = Value
  val NEW, REVIEWING, ESCALATED, CLOSED, REPORTED = Value
}

object RiskLevel extends Enumeration {
  type RiskLevel = Value
  val LOW, MEDIUM, HIGH, PROHIBITED = Value
}
