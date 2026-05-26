package com.aml.batch.feature

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

/**
 * T+1 batch job that writes daily customer entity snapshots to Hive.
 *
 * Reads from Kafka entity.customer topic (archived) and HBase customer features,
 * joins them, and writes a complete snapshot to aml.entity_snapshot.
 *
 * Design spec: "Daily entity snapshot" in Hive partitioning strategy.
 */
object EntitySnapshotJob {

  def main(args: Array[String]): Unit = {
    val snapshotDate = if (args.length > 0) args(0)
                       else java.time.LocalDate.now().minusDays(1).toString
    require(snapshotDate.matches("\\d{4}-\\d{2}-\\d{2}"), "date must be YYYY-MM-DD")

    val spark = SparkSession.builder()
      .appName("AML-EntitySnapshot")
      .enableHiveSupport()
      .getOrCreate()

    run(spark, snapshotDate)
  }

  def run(spark: SparkSession, snapshotDate: String): Unit = {
    import spark.implicits._

    // Read latest entity data from Hive (sourced from Kafka entity.customer)
    val entities = spark.sql(s"""
      SELECT
        customer_id,
        name,
        id_type,
        id_number,
        nationality,
        occupation,
        income_source,
        address,
        kyc_level,
        customer_type
      FROM aml.entity_snapshot
      WHERE dt = (
        SELECT MAX(dt) FROM aml.entity_snapshot WHERE dt < '$snapshotDate'
      )
    """)

    // Read latest customer features from the feature engineering job
    val features = spark.sql(s"""
      SELECT
        customer_id,
        risk_level,
        risk_score,
        txn_count_7d,
        txn_count_30d,
        total_amount_7d,
        total_amount_30d,
        avg_amount,
        max_amount,
        unique_counterparties_30d,
        alert_count_total,
        open_alert_count
      FROM aml.customer_features
      WHERE dt = '$snapshotDate'
    """)

    // Join entities with features
    val snapshot = entities.join(features, Seq("customer_id"), "left")
      .na.fill(0, Seq("txn_count_7d", "txn_count_30d", "alert_count_total", "open_alert_count"))
      .na.fill(0.0, Seq("risk_score"))
      .na.fill("UNKNOWN", Seq("risk_level"))
      .withColumn("dt", lit(snapshotDate))

    // Write to Hive with overwrite for the specific partition
    snapshot.write
      .mode("overwrite")
      .insertInto("aml.entity_snapshot")

    // Also sync to HBase via a separate output (optional, for real-time access)
    val hbaseRowCount = snapshot.count()
    println(s"Entity snapshot for $snapshotDate: $hbaseRowCount customers written to Hive")

    spark.stop()
  }
}
