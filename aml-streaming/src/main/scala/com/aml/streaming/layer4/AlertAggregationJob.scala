package com.aml.streaming.layer4

import com.aml.common.config.AppConfig
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AlertAggregationJob {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSession.builder()
      .appName("AML-Layer4-AlertAggregation")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, config)
  }

  def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    val alertStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", "alert.raw")
      .option("startingOffsets", "latest")
      .load()

    val alertSchema = new StructType()
      .add("alert_id", StringType)
      .add("txnId", StringType)
      .add("customerId", StringType)
      .add("alert_type", StringType)
      .add("severity", StringType)
      .add("rule_id", StringType)
      .add("rule_desc", StringType)
      .add("score", FloatType)
      .add("status", StringType)
      .add("created_at", TimestampType)

    val alerts = alertStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), alertSchema).as("alert"))
      .select("alert.*")

    // Deduplicate within time window
    val deduplicated = alerts
      .withWatermark("created_at", "30 minutes")
      .groupBy(
        col("customerId"),
        col("rule_id"),
        window(col("created_at"), "30 minutes")
      )
      .agg(
        first("alert_id").as("alert_id"),
        first("txnId").as("txnId"),
        max("severity").as("severity"),
        first("alert_type").as("alert_type"),
        first("rule_desc").as("rule_desc"),
        max("score").as("score"),
        count("*").as("alert_count"),
        min("created_at").as("created_at")
      )
      .withColumn("status", lit("NEW"))
      .withColumn("updated_at", current_timestamp())

    // Write to ClickHouse via JDBC
    val query = deduplicated.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", config.clickhouse.jdbcUrl)
          .option("dbtable", s"${config.clickhouse.database}.alert_queue")
          .option("user", config.clickhouse.username)
          .option("password", config.clickhouse.password)
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "/tmp/checkpoint/layer4-alerts")
      .outputMode("update")
      .start()

    query.awaitTermination()
  }
}
