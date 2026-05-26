package com.aml.common.serialization

import com.aml.common.model._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object KafkaSerDe {
  private val mapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def toJsonBytes(obj: Any): Array[Byte] = mapper.writeValueAsBytes(obj)
  def toJsonString(obj: Any): String = mapper.writeValueAsString(obj)
  def fromJsonString[T](json: String, clazz: Class[T]): T = mapper.readValue(json, clazz)
}

object TransactionSerDe {
  private val readMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def serialize(txn: CanonicalTransaction): Array[Byte] = {
    // Use map-based approach to avoid enum serialization issues with Jackson
    val map = Map(
      "txnId" -> txn.txnId,
      "txnType" -> txn.txnType.toString,
      "timestamp" -> txn.timestamp,
      "amount" -> txn.amount,
      "currency" -> txn.currency,
      "amountUsd" -> txn.amountUsd,
      "direction" -> txn.direction.toString,
      "customerId" -> txn.customerId,
      "counterpartyId" -> txn.counterpartyId,
      "channel" -> txn.channel,
      "countrySrc" -> txn.countrySrc,
      "countryDst" -> txn.countryDst,
      "metadata" -> txn.metadata,
      "rawPayload" -> txn.rawPayload
    )
    KafkaSerDe.toJsonBytes(map)
  }
  def deserialize(bytes: Array[Byte]): CanonicalTransaction = {
    val json = new String(bytes, "UTF-8")
    val node = readMapper.readTree(json)
    val metadataNode = node.path("metadata")
    import scala.collection.JavaConverters._
    val metadata: Map[String, String] = if (metadataNode.isObject) {
      metadataNode.fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
    } else {
      Map.empty
    }
    CanonicalTransaction(
      txnId = node.path("txnId").asText(),
      txnType = TxnType.withName(node.path("txnType").asText()),
      timestamp = node.path("timestamp").asLong(),
      amount = BigDecimal(node.path("amount").asText()),
      currency = node.path("currency").asText(),
      amountUsd = BigDecimal(node.path("amountUsd").asText()),
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
}

object AlertSerDe {
  def serialize(alert: Alert): Array[Byte] = {
    // Use snake_case keys to match ClickHouse alert_queue schema
    val map = Map(
      "alert_id" -> alert.alertId,
      "txn_id" -> alert.txnId,
      "customer_id" -> alert.customerId,
      "alert_type" -> alert.alertType.toString,
      "severity" -> alert.severity.toString,
      "rule_id" -> alert.ruleId,
      "rule_desc" -> alert.ruleDesc,
      "score" -> alert.score,
      "status" -> alert.status.toString,
      "created_at" -> alert.createdAt.toString,
      "updated_at" -> alert.updatedAt.toString,
      "reviewer_id" -> alert.reviewerId.orNull,
      "notes" -> alert.notes.orNull
    )
    KafkaSerDe.toJsonBytes(map)
  }
  private val readMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def deserialize(bytes: Array[Byte]): Alert = {
    val json = new String(bytes, "UTF-8")
    val node = readMapper.readTree(json)
    Alert(
      alertId = node.path("alert_id").asText(),
      txnId = node.path("txn_id").asText(),
      customerId = node.path("customer_id").asText(),
      alertType = AlertType.withName(node.path("alert_type").asText()),
      severity = Severity.withName(node.path("severity").asText()),
      ruleId = node.path("rule_id").asText(),
      ruleDesc = node.path("rule_desc").asText(),
      score = node.path("score").floatValue(),
      status = AlertStatus.withName(node.path("status").asText()),
      createdAt = java.time.Instant.parse(node.path("created_at").asText()),
      updatedAt = java.time.Instant.parse(node.path("updated_at").asText()),
      reviewerId = Option(node.path("reviewer_id")).filterNot(_.isMissingNode).filterNot(_.isNull).map(_.asText()),
      notes = Option(node.path("notes")).filterNot(_.isMissingNode).filterNot(_.isNull).map(_.asText())
    )
  }
}
