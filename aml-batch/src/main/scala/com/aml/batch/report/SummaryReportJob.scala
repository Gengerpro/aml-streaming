package com.aml.batch.report

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDate

object SummaryReportJob {

  def main(args: Array[String]): Unit = {
    val reportDate = if (args.length > 0) args(0) else LocalDate.now().minusDays(1).toString
    require(reportDate.matches("\\d{4}-\\d{2}-\\d{2}"), "reportDate must be in YYYY-MM-DD format")
    val spark = SparkSession.builder()
      .appName("AML-SummaryReport")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, reportDate)
  }

  def run(spark: SparkSession, reportDate: String): Unit = {
    import spark.implicits._

    val ctrSummary = spark.sql(s"""
      SELECT alert_id, txn_id, customer_id, rule_desc, created_at
      FROM aml.alert_history
      WHERE dt = '$reportDate' AND alert_type = 'CTR'
    """)

    val sarSummary = spark.sql(s"""
      SELECT alert_id, txn_id, customer_id, rule_desc, severity, created_at
      FROM aml.alert_history
      WHERE dt = '$reportDate' AND alert_type = 'SAR' AND status = 'REPORTED'
    """)

    val volumeStats = spark.sql(s"""
      SELECT
        COUNT(*) as total_transactions,
        SUM(amount_usd) as total_amount,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(CASE WHEN alert_type = 'CTR' THEN 1 END) as ctr_count,
        COUNT(CASE WHEN alert_type = 'SAR' THEN 1 END) as sar_count
      FROM aml.txn_normalized t
      LEFT JOIN aml.alert_history a ON t.txn_id = a.txn_id
      WHERE t.dt = '$reportDate'
    """)

    volumeStats.write
      .format("jdbc")
      .option("url", sys.env.getOrElse("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/aml"))
      .option("dbtable", "daily_summary")
      .option("user", sys.env.getOrElse("CLICKHOUSE_USER", "default"))
      .option("password", sys.env.getOrElse("CLICKHOUSE_PASSWORD", ""))
      .mode("append")
      .save()

    ctrSummary.write.mode("overwrite").insertInto("aml.ctr_reports")
    sarSummary.write.mode("overwrite").insertInto("aml.sar_reports")

    spark.stop()
  }
}
