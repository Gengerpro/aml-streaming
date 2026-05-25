package com.aml.batch.reconciliation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object AlertReconciliationJob {

  def main(args: Array[String]): Unit = {
    val reportDate = if (args.length > 0) args(0) else java.time.LocalDate.now().minusDays(1).toString
    val spark = SparkSession.builder()
      .appName("AML-AlertReconciliation")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, reportDate)
  }

  def run(spark: SparkSession, reportDate: String): Unit = {
    import spark.implicits._

    // Real-time alerts (from ClickHouse snapshot)
    val realtimeAlerts = spark.read
      .format("jdbc")
      .option("url", sys.env.getOrElse("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/aml"))
      .option("dbtable", s"(SELECT txn_id, alert_type, rule_id, created_at FROM alert_queue WHERE toDate(created_at) = '$reportDate')")
      .option("user", sys.env.getOrElse("CLICKHOUSE_USER", "default"))
      .option("password", sys.env.getOrElse("CLICKHOUSE_PASSWORD", ""))
      .load()

    // Batch-computed alerts (re-evaluate rules offline)
    val batchAlerts = spark.sql(s"""
      SELECT t.txn_id,
             CASE
               WHEN t.amount_usd >= 10000 AND t.channel IN ('ATM', 'BRANCH') THEN 'CTR'
               WHEN agg.txn_count_24h >= 5 AND agg.max_amount < 10000 AND agg.total_amount >= 40000 THEN 'SAR'
               ELSE NULL
             END as alert_type,
             CASE
               WHEN t.amount_usd >= 10000 AND t.channel IN ('ATM', 'BRANCH') THEN 'CTR-001'
               WHEN agg.txn_count_24h >= 5 AND agg.max_amount < 10000 AND agg.total_amount >= 40000 THEN 'SAR-001'
               ELSE NULL
             END as rule_id,
             t.txn_ts as created_at
      FROM aml.txn_normalized t
      JOIN (
        SELECT customer_id,
               COUNT(*) as txn_count_24h,
               MAX(amount_usd) as max_amount,
               SUM(amount_usd) as total_amount
        FROM aml.txn_normalized
        WHERE dt = '$reportDate'
        GROUP BY customer_id
      ) agg ON t.customer_id = agg.customer_id
      WHERE t.dt = '$reportDate'
        AND (t.amount_usd >= 10000 OR agg.txn_count_24h >= 5)
    """).filter(col("alert_type").isNotNull)

    // Compare
    val realtimeKeys = realtimeAlerts.select("txn_id", "alert_type").distinct()
    val batchKeys = batchAlerts.select("txn_id", "alert_type").distinct()

    val missingInRealtime = batchKeys.except(realtimeKeys)
    val missingCount = missingInRealtime.count()

    val extraInRealtime = realtimeKeys.except(batchKeys)
    val extraCount = extraInRealtime.count()

    val totalBatch = batchKeys.count()
    val diffRate = if (totalBatch > 0) missingCount.toDouble / totalBatch else 0.0

    val reconciliation = Seq(
      (reportDate, totalBatch, realtimeKeys.count(), missingCount, extraCount, diffRate)
    ).toDF("report_date", "batch_alerts", "realtime_alerts", "missing_in_realtime", "extra_in_realtime", "diff_rate")

    reconciliation.write
      .format("jdbc")
      .option("url", sys.env.getOrElse("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/aml"))
      .option("dbtable", "reconciliation_log")
      .option("user", sys.env.getOrElse("CLICKHOUSE_USER", "default"))
      .option("password", sys.env.getOrElse("CLICKHOUSE_PASSWORD", ""))
      .mode("append")
      .save()

    if (diffRate > 0.001) {
      println(s"WARNING: Reconciliation diff rate ${diffRate * 100}% exceeds 0.1% threshold!")
    }

    spark.stop()
  }
}
