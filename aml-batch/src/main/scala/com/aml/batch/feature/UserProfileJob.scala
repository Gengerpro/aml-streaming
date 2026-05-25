package com.aml.batch.feature

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

case class RiskFactors(
  countryRisk: Double,
  productRisk: Double,
  customerTypeRisk: Double,
  txnPatternRisk: Double
)

object UserProfileJob {

  val countryRiskWeights = Map(
    "IR" -> 1.0, "KP" -> 1.0, "SY" -> 0.9, "CU" -> 0.8,
    "US" -> 0.1, "GB" -> 0.1, "DE" -> 0.1, "FR" -> 0.1
  )

  def calculateRiskScore(factors: RiskFactors): Double = {
    val weights = (0.3, 0.25, 0.25, 0.2)
    val score = factors.countryRisk * weights._1 +
      factors.productRisk * weights._2 +
      factors.customerTypeRisk * weights._3 +
      factors.txnPatternRisk * weights._4
    Math.min(1.0, Math.max(0.0, score))
  }

  def riskLevel(score: Double): String = {
    if (score >= 0.7) "HIGH"
    else if (score >= 0.4) "MEDIUM"
    else "LOW"
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AML-UserProfile-FeatureEngineering")
      .enableHiveSupport()
      .getOrCreate()

    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val txns = spark.sql("""
      SELECT customer_id,
             COUNT(*) as txn_count_30d,
             SUM(amount_usd) as total_amount_30d,
             AVG(amount_usd) as avg_amount,
             MAX(amount_usd) as max_amount,
             COUNT(DISTINCT counterparty_id) as unique_counterparties_30d,
             COUNT(CASE WHEN txn_ts >= date_sub(current_date(), 7) THEN 1 END) as txn_count_7d,
             SUM(CASE WHEN txn_ts >= date_sub(current_date(), 7) THEN amount_usd ELSE 0 END) as total_amount_7d
      FROM aml.txn_normalized
      WHERE dt >= date_sub(current_date(), 30)
      GROUP BY customer_id
    """)

    val customers = spark.sql("""
      SELECT customer_id, name, nationality, occupation, customer_type
      FROM aml.entity_snapshot
      WHERE dt = current_date()
    """)

    val withRisk = customers.join(txns, Seq("customer_id"), "left")
      .na.fill(0)
      .withColumn("country_risk",
        when(col("nationality").isin("IR", "KP", "SY"), 1.0)
        .when(col("nationality").isin("US", "GB", "DE"), 0.1)
        .otherwise(0.5))
      .withColumn("txn_pattern_risk",
        when(col("txn_count_30d") > 100, 0.8)
        .when(col("txn_count_30d") > 50, 0.5)
        .otherwise(0.2))
      .withColumn("risk_score",
        (col("country_risk") * 0.3 + col("txn_pattern_risk") * 0.7))
      .withColumn("risk_level",
        when(col("risk_score") >= 0.7, "HIGH")
        .when(col("risk_score") >= 0.4, "MEDIUM")
        .otherwise("LOW"))

    withRisk.write
      .mode("overwrite")
      .insertInto("aml.customer_features")

    spark.stop()
  }
}
