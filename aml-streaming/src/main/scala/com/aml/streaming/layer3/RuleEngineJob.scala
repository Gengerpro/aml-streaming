package com.aml.streaming.layer3

import com.aml.common.config.AppConfig
import com.aml.common.model.{Alert, AlertStatus, AlertType, Severity}
import com.aml.common.serialization.AlertSerDe
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.Instant
import java.util.UUID

/**
 * Layer 3 job that bridges enriched transactions (Layer 2) to the alert pipeline.
 *
 * Reads from Kafka topic `txn.enriched`, evaluates each transaction against
 * AML rules loaded from YAML, and writes triggered alerts to Kafka topic `alert.raw`.
 */
object RuleEngineJob {

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
      .option("checkpointLocation", "/tmp/checkpoint/layer3-rule-engine")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  private def processBatch(batchDF: DataFrame, batchId: Long, config: AppConfig): Unit = {
    val spark = batchDF.sparkSession

    if (batchDF.isEmpty) return

    val rows = batchDF.collect()
    val ruleEngine = RuleEngine.loadFromYaml(config.ruleEngine.ruleStorePath)
    val now = Instant.now()

    val alerts: Seq[Array[Byte]] = rows.flatMap { row =>
      val data = rowToMap(row)
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
      }.map(AlertSerDe.serialize)
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
