package com.aml.streaming.layer2

import com.aml.common.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FeatureEnrichmentJob {

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

    // Window aggregations by customer_id
    val withWatermark = txns
      .withWatermark("process_ts", "1 hour")

    // 1-hour window aggregation
    val windowed1h = withWatermark
      .groupBy(
        col("customerId"),
        window(col("process_ts"), "1 hour")
      )
      .agg(
        count("*").as("txn_count_1h"),
        sum("amountUsd").as("total_amount_1h"),
        max("amountUsd").as("max_amount_1h"),
        countDistinct("counterpartyId").as("unique_counterparties_1h")
      )

    // 24-hour window aggregation
    val windowed24h = withWatermark
      .groupBy(
        col("customerId"),
        window(col("process_ts"), "24 hours")
      )
      .agg(
        count("*").as("txn_count_24h"),
        sum("amountUsd").as("total_amount_24h"),
        max("amountUsd").as("max_amount_24h"),
        countDistinct("counterpartyId").as("unique_counterparties_24h")
      )

    // Join original transaction with window features
    val enriched = txns
      .join(windowed1h, Seq("customerId"), "left")
      .join(windowed24h, Seq("customerId"), "left")
      .withColumn("enriched_ts", current_timestamp())
      .selectExpr("to_json(struct(*)) AS value")

    // Write to enriched topic
    val query = enriched
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("topic", "txn.enriched")
      .option("checkpointLocation", "/tmp/checkpoint/layer2-enriched")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
