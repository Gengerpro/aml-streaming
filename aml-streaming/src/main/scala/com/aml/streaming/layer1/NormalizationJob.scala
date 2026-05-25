package com.aml.streaming.layer1

import com.aml.common.config.AppConfig
import com.aml.common.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NormalizationJob {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSession.builder()
      .appName("AML-Layer1-Normalization")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, config)
  }

  def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    // Read from multiple Kafka topics
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", "txn.bank,txn.card,txn.forex")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Parse JSON value
    val txnSchema = new StructType()
      .add("txnId", StringType)
      .add("txnType", StringType)
      .add("timestamp", LongType)
      .add("amount", DecimalType(18, 2))
      .add("currency", StringType)
      .add("amountUsd", DecimalType(18, 2))
      .add("direction", StringType)
      .add("customerId", StringType)
      .add("counterpartyId", StringType)
      .add("channel", StringType)
      .add("countrySrc", StringType)
      .add("countryDst", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("rawPayload", StringType)

    val parsed = rawStream
      .selectExpr("CAST(value AS STRING) as json_str", "topic", "timestamp as kafka_ts")
      .select(from_json(col("json_str"), txnSchema).as("txn"), col("topic"), col("kafka_ts"))
      .select("txn.*", "topic", "kafka_ts")

    // Validate & standardize
    val validated = parsed
      .filter(col("txnId").isNotNull && col("txnId") =!= "")
      .filter(col("amount") > 0)
      .filter(col("customerId").isNotNull && col("customerId") =!= "")
      .withColumn("currency", upper(col("currency")))
      .withColumn("txn_type_normalized",
        when(col("txnType") === "TRANSFER", lit("TRANSFER"))
        .when(col("txnType") === "CARD", lit("CARD"))
        .when(col("txnType") === "FOREX", lit("FOREX"))
        .otherwise(lit("UNKNOWN")))
      .withColumn("direction_normalized",
        when(col("direction") === "INBOUND", lit("INBOUND"))
        .when(col("direction") === "OUTBOUND", lit("OUTBOUND"))
        .otherwise(lit("UNKNOWN")))
      .withColumn("process_ts", current_timestamp())
      // Tag for bypass routing
      .withColumn("is_large_txn", col("amount") >= lit(config.ruleEngine.ctrThreshold))
      .withColumn("route_tag",
        when(col("is_large_txn"), lit("BYPASS_CTR"))
        .otherwise(lit("MAIN")))

    // Write valid transactions to normalized topic
    val query = validated
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("topic", "txn.normalized")
      .option("checkpointLocation", "/tmp/checkpoint/layer1-normalized")
      .outputMode("append")
      .start()

    // Write invalid records to Dead Letter Queue
    val invalidRecords = parsed
      .filter(
        col("txnId").isNull || col("txnId") === "" ||
        col("amount") <= 0 ||
        col("customerId").isNull || col("customerId") === ""
      )

    val dlqQuery = invalidRecords
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("topic", "txn.dlq")
      .option("checkpointLocation", "/tmp/checkpoint/layer1-dlq")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
