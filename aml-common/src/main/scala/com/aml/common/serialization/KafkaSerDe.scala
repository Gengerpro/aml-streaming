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
    // Use a map-based approach to avoid enum serialization issues
    val map = Map(
      "alertId" -> alert.alertId,
      "txnId" -> alert.txnId,
      "customerId" -> alert.customerId,
      "alertType" -> alert.alertType.toString,
      "severity" -> alert.severity.toString,
      "ruleId" -> alert.ruleId,
      "ruleDesc" -> alert.ruleDesc,
      "score" -> alert.score,
      "status" -> alert.status.toString,
      "createdAt" -> alert.createdAt.toString,
      "updatedAt" -> alert.updatedAt.toString,
      "reviewerId" -> alert.reviewerId.orNull,
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
      alertId = node.get("alertId").asText(),
      txnId = node.get("txnId").asText(),
      customerId = node.get("customerId").asText(),
      alertType = AlertType.withName(node.get("alertType").asText()),
      severity = Severity.withName(node.get("severity").asText()),
      ruleId = node.get("ruleId").asText(),
      ruleDesc = node.get("ruleDesc").asText(),
      score = node.get("score").floatValue(),
      status = AlertStatus.withName(node.get("status").asText()),
      createdAt = java.time.Instant.parse(node.get("createdAt").asText()),
      updatedAt = java.time.Instant.parse(node.get("updatedAt").asText()),
      reviewerId = Option(node.get("reviewerId")).filterNot(_.isNull).map(_.asText()),
      notes = Option(node.get("notes")).filterNot(_.isNull).map(_.asText())
    )
  }
}
