package com.aml.common.model

case class CustomerProfile(
  customerId: String,
  name: String,
  idType: String,
  idNumber: String,
  nationality: String,
  occupation: String,
  incomeSource: String,
  address: String,
  kycLevel: String,
  riskLevel: RiskLevel.Value,
  riskScore: Double,
  txnCount7d: Long,
  txnCount30d: Long,
  totalAmount7d: BigDecimal,
  totalAmount30d: BigDecimal,
  avgAmount: BigDecimal,
  maxAmount: BigDecimal,
  uniqueCounterparties30d: Int,
  alertCountTotal: Long,
  alertCount90d: Long,
  openAlertCount: Int
)
