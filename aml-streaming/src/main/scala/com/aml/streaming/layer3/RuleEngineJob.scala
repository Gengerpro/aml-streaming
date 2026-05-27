package com.aml.streaming.layer3

import com.aml.common.config.AppConfig
import com.aml.common.model.{Alert, AlertStatus, AlertType, Severity}
import com.aml.common.serialization.AlertSerDe
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File
import java.time.Instant
import java.util.UUID
import scala.io.Source

/**
 * Layer 3 job that bridges enriched transactions (Layer 2) to the alert pipeline.
 *
 * Reads from Kafka topic `txn.enriched`, evaluates each transaction against
 * AML rules loaded from YAML, screens counterparties against sanctions lists,
 * and writes triggered alerts to Kafka topic `alert.raw`.
 */
object RuleEngineJob {

  // Cached rule engine with periodic refresh (single volatile pair avoids TOCTOU race)
  @volatile private var cachedState: (RuleEngine, Long) = _

  // Cached sanctions screener
  @volatile private var cachedScreener: SanctionsScreener = _

  private def getRuleEngine(config: AppConfig): RuleEngine = {
    val now = System.currentTimeMillis()
    val refreshIntervalMs = config.ruleEngine.refreshIntervalSeconds * 1000L
    val state = cachedState
    if (state == null || (now - state._2) > refreshIntervalMs) {
      synchronized {
        val state2 = cachedState
        if (state2 == null || (now - state2._2) > refreshIntervalMs) {
          val engine = RuleEngine.loadFromYaml(config.ruleEngine.ruleStorePath)
          cachedState = (engine, now)
          engine
        } else {
          state2._1
        }
      }
    } else {
      state._1
    }
  }

  private def getSanctionsScreener(config: AppConfig): SanctionsScreener = {
    if (cachedScreener == null) {
      val names = loadSanctionsList(config.ruleEngine.sanctionsListPath)
      cachedScreener = SanctionsScreener.create(names)
    }
    cachedScreener
  }

  private def loadSanctionsList(path: String): List[String] = {
    val file = new File(path)
    if (file.exists()) {
      Source.fromFile(file, "UTF-8")
        .getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(!_.startsWith("#"))
        .toList
    } else {
      // Fallback: embedded sample sanctions list for development/testing
      List(
        "AL QAIDA", "ISIS", "TALIBAN", "HEZBOLLAH", "HAMAS",
        "IRANIAN REVOLUTIONARY GUARD", "KOREA KWANGSON BANKING",
        "BANK SEPAH", "BANK MELLI"
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSession.builder()
      .appName("AML-Layer3-RuleEngine")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, config)
  }

  def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    val enrichedStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", "txn.enriched")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Schema must match the fields emitted by FeatureEnrichmentJob (mapGroupsWithState output)
    val enrichedSchema = new StructType()
      .add("txnId", StringType)
      .add("txn_type_normalized", StringType)
      .add("timestamp", LongType)
      .add("amountUsd", DecimalType(18, 2))
      .add("counterpartyId", StringType)
      .add("customerId", StringType)
      // rolling window aggregation fields
      .add("txn_count_1h", LongType)
      .add("total_amount_1h", DecimalType(18, 2))
      .add("max_amount_1h", DecimalType(18, 2))
      .add("unique_counterparties_1h", LongType)
      .add("txn_count_24h", LongType)
      .add("total_amount_24h", DecimalType(18, 2))
      .add("max_amount_24h", DecimalType(18, 2))
      .add("unique_counterparties_24h", LongType)
      // customer profile fields from HBase
      .add("risk_level", StringType)
      .add("risk_score", DoubleType)
      .add("kyc_status", StringType)
      .add("alert_count_total", LongType)
      .add("open_alert_count", IntegerType)
      .add("route_tag", StringType)
      .add("enriched_ts", TimestampType)

    val parsed = enrichedStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), enrichedSchema).as("enriched"))
      .select("enriched.*")

    // Write alerts to Kafka using foreachBatch for per-row rule evaluation
    val query = parsed.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatch(batchDF, batchId, config)
      }
      .option("checkpointLocation", s"${config.checkpoint.basePath}/layer3-rule-engine")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  private def processBatch(batchDF: DataFrame, batchId: Long, config: AppConfig): Unit = {
    val spark = batchDF.sparkSession

    if (batchDF.isEmpty) return

    val rows = batchDF.collect()
    val ruleEngine = getRuleEngine(config)
    val screener = getSanctionsScreener(config)
    val now = Instant.now()

    val alerts: Seq[Array[Byte]] = rows.flatMap { row =>
      val data = rowToMap(row)
      val routeTag = data.getOrElse("route_tag", "MAIN").toString

      // BYPASS_CTR: large cash transactions skip rule engine, auto-generate CTR
      val ctrBypassAlerts = if (routeTag == "BYPASS_CTR") {
        Seq(Alert(
          alertId = UUID.randomUUID().toString,
          txnId = data.getOrElse("txnId", "").toString,
          customerId = data.getOrElse("customerId", "").toString,
          alertType = AlertType.CTR,
          severity = Severity.HIGH,
          ruleId = "CTR-001",
          ruleDesc = s"Auto-CTR: Large transaction ${data.getOrElse("amountUsd", "N/A")} bypass lane",
          score = 1.0f,
          status = AlertStatus.NEW,
          createdAt = now,
          updatedAt = now
        ))
      } else Seq.empty

      // Rule engine evaluation (skip for pure bypass lane)
      val ruleAlerts = if (routeTag != "BYPASS_CTR") {
        ruleEngine.evaluate(data).map { match_ =>
          Alert(
            alertId = UUID.randomUUID().toString,
            txnId = data.getOrElse("txnId", "").toString,
            customerId = data.getOrElse("customerId", "").toString,
            alertType = AlertType.withName(match_.ruleType),
            severity = Severity.withName(match_.severity),
            ruleId = match_.id,
            ruleDesc = match_.message,
            score = match_.score,
            status = AlertStatus.NEW,
            createdAt = now,
            updatedAt = now
          )
        }
      } else Seq.empty

      // Sanctions screening on counterparty (always runs, even for bypass)
      val counterpartyId = data.getOrElse("counterpartyId", "").toString
      val sanctionAlerts = if (counterpartyId.nonEmpty) {
        screener.screen(counterpartyId) match {
          case ScreeningResult(true, Some(match_)) =>
            Seq(Alert(
              alertId = UUID.randomUUID().toString,
              txnId = data.getOrElse("txnId", "").toString,
              customerId = data.getOrElse("customerId", "").toString,
              alertType = AlertType.SANCTION,
              severity = Severity.CRITICAL,
              ruleId = "SAN-001",
              ruleDesc = s"Sanctions match: '${match_.matchedName}' (similarity: ${f"${match_.similarity}%.2f"})",
              score = match_.similarity.toFloat,
              status = AlertStatus.NEW,
              createdAt = now,
              updatedAt = now
            ))
          case _ => Seq.empty
        }
      } else Seq.empty

      (ctrBypassAlerts ++ ruleAlerts ++ sanctionAlerts).map(AlertSerDe.serialize)
    }

    if (alerts.nonEmpty) {
      val schema = StructType(Seq(StructField("value", BinaryType)))
      val rdd = spark.sparkContext.parallelize(alerts.map(Row(_)))
      val alertDF = spark.createDataFrame(rdd, schema)
      alertDF.write
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
        .option("topic", "alert.raw")
        .save()
    }
  }

  /** Convert a Spark Row into a Map[String, Any] for rule evaluation. */
  private def rowToMap(row: Row): Map[String, Any] = {
    val fields = row.schema.fieldNames
    fields.flatMap { field =>
      val idx = row.fieldIndex(field)
      if (row.isNullAt(idx)) None
      else {
        val value = row.get(idx) match {
          case d: java.math.BigDecimal => BigDecimal(d)
          case bd: org.apache.spark.sql.types.Decimal => BigDecimal(bd.toJavaBigDecimal)
          case ts: java.sql.Timestamp => ts.toInstant.toString
          case other => other
        }
        Some(field -> value)
      }
    }.toMap
  }
}
