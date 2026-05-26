package com.aml.streaming.layer2

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import scala.util.Try

case class CustomerFeatures(
  customerId: String,
  riskLevel: String,
  riskScore: Double,
  kycStatus: String,
  txnCount7d: Long,
  txnCount30d: Long,
  totalAmount7d: Double,
  totalAmount30d: Double,
  avgAmount: Double,
  maxAmount: Double,
  uniqueCounterparties30d: Int,
  alertCountTotal: Long,
  openAlertCount: Int
)

class HBaseClient(zookeeperQuorum: String, zookeeperPort: Int, tableName: String) {
  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
  conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort)

  // Reuse a single connection (thread-safe) instead of creating per query
  @volatile private lazy val connection = ConnectionFactory.createConnection(conf)

  def close(): Unit = {
    if (connection != null && !connection.isClosed) connection.close()
  }

  def getCustomerFeatures(customerId: String): Option[CustomerFeatures] = {
    Try {
      val table = connection.getTable(TableName.valueOf(tableName))
      try {
        val rowKey = s"${customerId.hashCode.abs % 1000}_$customerId"
        val get = new Get(Bytes.toBytes(rowKey))
        get.addFamily(Bytes.toBytes("cf:risk"))
        get.addFamily(Bytes.toBytes("cf:profile"))
        get.addFamily(Bytes.toBytes("cf:txn_stats"))
        get.addFamily(Bytes.toBytes("cf:alert"))
        val result = table.get(get)
        if (result.isEmpty) None
        else Some(parseCustomerFeatures(result, customerId))
      } finally {
        table.close()
      }
    }.getOrElse(None)
  }

  private def parseCustomerFeatures(result: Result, customerId: String): CustomerFeatures = {
    def getValue(cf: String, col: String): Option[Array[Byte]] =
      Option(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(col)))

    CustomerFeatures(
      customerId = customerId,
      riskLevel = getValue("cf:risk", "risk_level").map(Bytes.toString).getOrElse("UNKNOWN"),
      riskScore = getValue("cf:risk", "risk_score").map(Bytes.toDouble).getOrElse(0.0),
      kycStatus = getValue("cf:profile", "kyc_level").map(Bytes.toString).getOrElse("UNKNOWN"),
      txnCount7d = getValue("cf:txn_stats", "txn_count_7d").map(Bytes.toLong).getOrElse(0L),
      txnCount30d = getValue("cf:txn_stats", "txn_count_30d").map(Bytes.toLong).getOrElse(0L),
      totalAmount7d = getValue("cf:txn_stats", "total_amount_7d").map(Bytes.toDouble).getOrElse(0.0),
      totalAmount30d = getValue("cf:txn_stats", "total_amount_30d").map(Bytes.toDouble).getOrElse(0.0),
      avgAmount = getValue("cf:txn_stats", "avg_amount").map(Bytes.toDouble).getOrElse(0.0),
      maxAmount = getValue("cf:txn_stats", "max_amount").map(Bytes.toDouble).getOrElse(0.0),
      uniqueCounterparties30d = getValue("cf:txn_stats", "unique_counterparties_30d").map(Bytes.toInt).getOrElse(0),
      alertCountTotal = getValue("cf:alert", "alert_count_total").map(Bytes.toLong).getOrElse(0L),
      openAlertCount = getValue("cf:alert", "open_alert_count").map(Bytes.toInt).getOrElse(0)
    )
  }
}
