package com.aml.common.config

import com.typesafe.config.{Config, ConfigFactory}

case class KafkaConfig(
  bootstrapServers: String,
  groupId: String,
  schemaRegistryUrl: Option[String]
)

case class HBaseConfig(
  zookeeperQuorum: String,
  zookeeperPort: Int,
  tableName: String
)

case class ClickHouseConfig(
  jdbcUrl: String,
  username: String,
  password: String,
  database: String
)

case class RedisConfig(
  host: String,
  port: Int,
  password: Option[String],
  db: Int
)

case class HiveConfig(
  metastoreUris: String,
  warehouseDir: String
)

case class RuleEngineConfig(
  ruleStorePath: String,
  refreshIntervalSeconds: Int,
  ctrThreshold: BigDecimal,
  highRiskCountries: List[String],
  sanctionsListPath: String
)

case class CheckpointConfig(
  basePath: String
)

case class AppConfig(
  kafka: KafkaConfig,
  hbase: HBaseConfig,
  clickhouse: ClickHouseConfig,
  redis: RedisConfig,
  hive: HiveConfig,
  checkpoint: CheckpointConfig,
  ruleEngine: RuleEngineConfig
) {
  def kafkaBootstrapServers: String = kafka.bootstrapServers
  def kafkaGroupId: String = kafka.groupId
}

object AppConfig {
  def load(resourcePath: String = "application.conf"): AppConfig = {
    val config = ConfigFactory.load(resourcePath)
    fromConfig(config)
  }

  def fromConfig(config: Config): AppConfig = {
    AppConfig(
      kafka = KafkaConfig(
        bootstrapServers = config.getString("aml.kafka.bootstrap-servers"),
        groupId = config.getString("aml.kafka.group-id"),
        schemaRegistryUrl = if (config.hasPath("aml.kafka.schema-registry-url"))
          Some(config.getString("aml.kafka.schema-registry-url")) else None
      ),
      hbase = HBaseConfig(
        zookeeperQuorum = config.getString("aml.hbase.zookeeper-quorum"),
        zookeeperPort = config.getInt("aml.hbase.zookeeper-port"),
        tableName = config.getString("aml.hbase.table-name")
      ),
      clickhouse = ClickHouseConfig(
        jdbcUrl = config.getString("aml.clickhouse.jdbc-url"),
        username = config.getString("aml.clickhouse.username"),
        password = config.getString("aml.clickhouse.password"),
        database = config.getString("aml.clickhouse.database")
      ),
      redis = RedisConfig(
        host = config.getString("aml.redis.host"),
        port = config.getInt("aml.redis.port"),
        password = if (config.hasPath("aml.redis.password"))
          Some(config.getString("aml.redis.password")) else None,
        db = config.getInt("aml.redis.db")
      ),
      hive = HiveConfig(
        metastoreUris = config.getString("aml.hive.metastore-uris"),
        warehouseDir = config.getString("aml.hive.warehouse-dir")
      ),
      checkpoint = CheckpointConfig(
        basePath = config.getString("aml.checkpoint.base-path")
      ),
      ruleEngine = RuleEngineConfig(
        ruleStorePath = config.getString("aml.rule-engine.rule-store-path"),
        refreshIntervalSeconds = config.getInt("aml.rule-engine.refresh-interval-seconds"),
        ctrThreshold = BigDecimal(config.getString("aml.rule-engine.ctr-threshold")),
        highRiskCountries = config.getStringList("aml.rule-engine.high-risk-countries").toArray.toList.map(_.toString),
        sanctionsListPath = if (config.hasPath("aml.rule-engine.sanctions-list-path"))
          config.getString("aml.rule-engine.sanctions-list-path") else "/aml/sanctions/sdn.csv"
      )
    )
  }
}
