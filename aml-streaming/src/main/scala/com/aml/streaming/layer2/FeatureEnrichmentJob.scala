package com.aml.streaming.layer2

import com.aml.common.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._

// Domain case classes used for stateful mapGroupsWithState processing
case class RawTransaction(
  txnId: String,
  txn_type_normalized: String,
  timestamp: Long,
  amountUsd: BigDecimal,
  counterpartyId: String,
  customerId: String,
  process_ts: java.sql.Timestamp
)

case class TxnAggState(
  buffer: Seq[RawTransaction],
  lastUpdatedMs: Long
)

case class EnrichedTransaction(
  txnId: String,
  txn_type_normalized: String,
  timestamp: Long,
  amountUsd: BigDecimal,
  counterpartyId: String,
  customerId: String,
  txn_count_1h: Long,
  total_amount_1h: BigDecimal,
  max_amount_1h: BigDecimal,
  unique_counterparties_1h: Long,
  txn_count_24h: Long,
  total_amount_24h: BigDecimal,
  max_amount_24h: BigDecimal,
  unique_counterparties_24h: Long,
  enriched_ts: java.sql.Timestamp
)

object FeatureEnrichmentJob {

  private val WINDOW_1H_MS  = 3600000L      // 1 hour in ms
  private val WINDOW_24H_MS = 86400000L     // 24 hours in ms
  private val MAX_STATE_AGE = WINDOW_24H_MS * 2 // evict state idle > 48h

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSession.builder()
      .appName("AML-Layer2-FeatureEnrichment")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, config)
  }

  def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    // Read normalized transactions
    val normalizedStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", "txn.normalized")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val txnSchema = new StructType()
      .add("txnId", StringType)
      .add("txn_type_normalized", StringType)
      .add("timestamp", LongType)
      .add("amount", DecimalType(18, 2))
      .add("currency", StringType)
      .add("amountUsd", DecimalType(18, 2))
      .add("direction_normalized", StringType)
      .add("customerId", StringType)
      .add("counterpartyId", StringType)
      .add("channel", StringType)
      .add("countrySrc", StringType)
      .add("countryDst", StringType)
      .add("route_tag", StringType)
      .add("process_ts", TimestampType)

    val txns = normalizedStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), txnSchema).as("txn"))
      .select("txn.*")

    // Key by customerId and apply stateful per-customer rolling aggregation.
    // This replaces the broken windowed-join approach (which produced a Cartesian
    // product by joining on customerId alone) with mapGroupsWithState that
    // maintains a bounded buffer of recent transactions per customer and computes
    // rolling 1h and 24h aggregates directly for each incoming transaction.
    val enriched = txns
      .select(
        col("txnId"),
        col("txn_type_normalized"),
        col("timestamp"),
        col("amountUsd").cast(DecimalType(18, 2)).as("amountUsd"),
        col("counterpartyId"),
        col("customerId"),
        col("process_ts")
      )
      .as[RawTransaction]
      .groupByKey(_.customerId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updateState)

    val query = enriched
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("topic", "txn.enriched")
      .option("checkpointLocation", "/tmp/checkpoint/layer2-enriched")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  /** Stateful function: computes rolling 1h/24h aggregates per customer. */
  private def updateState(
    customerId: String,
    txns: Iterator[RawTransaction],
    state: GroupState[TxnAggState]
  ): Iterator[EnrichedTransaction] = {

    // Evict stale state (no activity for 2x the largest window)
    if (state.exists && !state.hasTimedOut) {
      val age = System.currentTimeMillis() - state.get.lastUpdatedMs
      if (age > MAX_STATE_AGE) state.remove()
    }

    val existing: Seq[RawTransaction] =
      if (state.exists) state.get.buffer else Seq.empty

    val nowMs = System.currentTimeMillis()

    // Append new transactions and evict entries older than 24h
    val updatedBuffer = (existing ++ txns)
      .filter(t => nowMs - t.process_ts.getTime < WINDOW_24H_MS)

    val newState = TxnAggState(updatedBuffer, nowMs)
    state.update(newState)

    // Emit one EnrichedTransaction per incoming transaction
    txns.map { txn =>
      val txnTime = txn.process_ts.getTime

      val recent1h = updatedBuffer.filter(t => txnTime - t.process_ts.getTime < WINDOW_1H_MS)
      val recent24h = updatedBuffer.filter(t => txnTime - t.process_ts.getTime < WINDOW_24H_MS)

      EnrichedTransaction(
        txnId = txn.txnId,
        txn_type_normalized = txn.txn_type_normalized,
        timestamp = txn.timestamp,
        amountUsd = txn.amountUsd,
        counterpartyId = txn.counterpartyId,
        customerId = txn.customerId,
        txn_count_1h               = recent1h.size.toLong,
        total_amount_1h            = recent1h.map(_.amountUsd).sum,
        max_amount_1h              = if (recent1h.nonEmpty) recent1h.map(_.amountUsd).max else BigDecimal(0),
        unique_counterparties_1h   = recent1h.map(_.counterpartyId).distinct.size.toLong,
        txn_count_24h              = recent24h.size.toLong,
        total_amount_24h           = recent24h.map(_.amountUsd).sum,
        max_amount_24h             = if (recent24h.nonEmpty) recent24h.map(_.amountUsd).max else BigDecimal(0),
        unique_counterparties_24h  = recent24h.map(_.counterpartyId).distinct.size.toLong,
        enriched_ts = new java.sql.Timestamp(nowMs)
      )
    }
  }
}
