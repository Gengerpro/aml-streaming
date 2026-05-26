package com.aml.streaming.layer4

import com.aml.common.config.AppConfig
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

object AlertAggregationJob {

  /** Map severity string to numeric order (matching AlertDeduplicator). */
  val severityToNum: UserDefinedFunction = udf((s: String) => s match {
    case "LOW"      => 0
    case "MEDIUM"   => 1
    case "HIGH"     => 2
    case "CRITICAL" => 3
    case _          => -1
  })

  /** Map numeric severity order back to string. */
  val numToSeverity: UserDefinedFunction = udf((n: Int) => n match {
    case 3 => "CRITICAL"
    case 2 => "HIGH"
    case 1 => "MEDIUM"
    case _ => "LOW"
  })

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
        max(severityToNum(col("severity"))).as("severity_num"),
        first("alert_type").as("alert_type"),
        first("rule_desc").as("rule_desc"),
        max("score").as("score"),
        min("created_at").as("created_at")
      )
      .withColumn("severity", numToSeverity(col("severity_num")))
      .drop("severity_num")
      .withColumn("status", lit("NEW"))
      .withColumn("updated_at", current_timestamp())
      // Rename camelCase Kafka fields to snake_case to match ClickHouse schema
      .withColumnRenamed("txnId", "txn_id")
      .withColumnRenamed("customerId", "customer_id")

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
