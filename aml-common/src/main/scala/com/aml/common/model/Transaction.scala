package com.aml.common.model

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._

case class CanonicalTransaction(
  txnId: String,
  txnType: TxnType.Value,
  timestamp: Long,
  amount: BigDecimal,
  currency: String,
  amountUsd: BigDecimal,
  direction: Direction.Value,
  customerId: String,
  counterpartyId: String,
  channel: String,
  countrySrc: String,
  countryDst: String,
  metadata: Map[String, String],
  rawPayload: String
)

object CanonicalTransaction {
  private val mapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def fromJson(json: String): CanonicalTransaction = {
    val node = mapper.readTree(json)
    val metadataNode = node.path("metadata")
    val metadata: Map[String, String] = if (metadataNode.isObject) {
      metadataNode.fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
    } else {
      Map.empty
    }
    CanonicalTransaction(
      txnId = node.path("txnId").asText(),
      txnType = TxnType.withName(node.path("txnType").asText()),
      timestamp = node.path("timestamp").asLong(),
      amount = BigDecimal(node.path("amount").asDouble()),
      currency = node.path("currency").asText(),
      amountUsd = BigDecimal(node.path("amountUsd").asDouble()),
      direction = Direction.withName(node.path("direction").asText()),
      customerId = node.path("customerId").asText(),
      counterpartyId = node.path("counterpartyId").asText(),
      channel = node.path("channel").asText(),
      countrySrc = node.path("countrySrc").asText(),
      countryDst = node.path("countryDst").asText(),
      metadata = metadata,
      rawPayload = node.path("rawPayload").asText()
    )
  }

  def toJson(txn: CanonicalTransaction): String = {
    mapper.writeValueAsString(txn)
  }
}
