package com.aml.common.model

import java.time.Instant

case class Alert(
  alertId: String,
  txnId: String,
  customerId: String,
  alertType: AlertType.Value,
  severity: Severity.Value,
  ruleId: String,
  ruleDesc: String,
  score: Float,
  status: AlertStatus.Value,
  createdAt: Instant,
  updatedAt: Instant,
  reviewerId: Option[String] = None,
  notes: Option[String] = None
)

case class CtrEvent(
  ctrId: String,
  txnId: String,
  customerId: String,
  amount: BigDecimal,
  currency: String,
  channel: String,
  createdAt: Instant
)

case class SanctionHit(
  hitId: String,
  txnId: String,
  customerId: String,
  counterpartyId: String,
  matchedName: String,
  listSource: String,
  similarity: Double,
  createdAt: Instant
)
