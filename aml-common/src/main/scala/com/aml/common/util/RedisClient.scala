package com.aml.common.util

import com.aml.common.config.RedisConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis client utility for AML system.
 *
 * Provides:
 * - Rule cache (key-value)
 * - Sorted sets for rule version timeline
 * - Bloom Filter serialization/deserialization
 * - Customer feature cache
 */
class RedisClient(config: RedisConfig) {

  private val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(20)
  poolConfig.setMaxIdle(10)
  poolConfig.setMinIdle(2)

  private val pool = new JedisPool(poolConfig, config.host, config.port, 2000,
    config.password.orNull, config.db)

  private def withJedis[T](f: Jedis => T): T = {
    val jedis = pool.getResource
    try {
      f(jedis)
    } finally {
      jedis.close()
    }
  }

  // ── Key-Value Operations ────────────────────────────────────────

  def get(key: String): Option[String] = withJedis { jedis =>
    Option(jedis.get(key))
  }

  def set(key: String, value: String): Unit = withJedis { jedis =>
    jedis.set(key, value)
  }

  def setex(key: String, ttlSeconds: Int, value: String): Unit = withJedis { jedis =>
    jedis.setex(key, ttlSeconds, value)
  }

  def del(key: String): Unit = withJedis { jedis =>
    jedis.del(key)
  }

  def exists(key: String): Boolean = withJedis { jedis =>
    jedis.exists(key)
  }

  // ── Hash Operations ─────────────────────────────────────────────

  def hset(key: String, field: String, value: String): Unit = withJedis { jedis =>
    jedis.hset(key, field, value)
  }

  def hget(key: String, field: String): Option[String] = withJedis { jedis =>
    Option(jedis.hget(key, field))
  }

  def hgetAll(key: String): Map[String, String] = withJedis { jedis =>
    import scala.collection.JavaConverters._
    jedis.hgetAll(key).asScala.toMap
  }

  // ── Sorted Set Operations (Rule Version Timeline) ───────────────

  def zadd(key: String, score: Double, member: String): Unit = withJedis { jedis =>
    jedis.zadd(key, score, member)
  }

  def zrevrange(key: String, start: Long, stop: Long): List[String] = withJedis { jedis =>
    import scala.collection.JavaConverters._
    jedis.zrevrange(key, start, stop).asScala.toList
  }

  def zrevrangeByScore(key: String, max: Double, min: Double): List[String] = withJedis { jedis =>
    import scala.collection.JavaConverters._
    jedis.zrevrangeByScore(key, max, min).asScala.toList
  }

  // ── Bloom Filter Persistence ────────────────────────────────────

  def setBytes(key: String, data: Array[Byte], ttlSeconds: Int): Unit = withJedis { jedis =>
    jedis.setex(key.getBytes("UTF-8"), ttlSeconds, data)
  }

  def getBytes(key: String): Option[Array[Byte]] = withJedis { jedis =>
    Option(jedis.get(key.getBytes("UTF-8")))
  }

  // ── Customer Feature Cache ──────────────────────────────────────

  def cacheCustomerFeatures(customerId: String, features: Map[String, String], ttlSeconds: Int = 3600): Unit = {
    val key = s"aml:customer:$customerId:features"
    withJedis { jedis =>
      import scala.collection.JavaConverters._
      jedis.hmset(key, features.asJava)
      jedis.expire(key, ttlSeconds)
    }
  }

  def getCustomerFeatures(customerId: String): Option[Map[String, String]] = {
    val key = s"aml:customer:$customerId:features"
    val result = hgetAll(key)
    if (result.isEmpty) None else Some(result)
  }

  // ── Rule Cache ──────────────────────────────────────────────────

  def cacheRules(versionId: String, rulesJson: String, ttlSeconds: Int = 86400): Unit = {
    setex(s"aml:rules:$versionId", ttlSeconds, rulesJson)
    // Also update the sorted set index
    zadd("aml:rules:versions", System.currentTimeMillis().toDouble, versionId)
  }

  def getLatestRuleVersion: Option[String] = {
    val versions = zrevrange("aml:rules:versions", 0, 0)
    versions.headOption.flatMap(vid => get(s"aml:rules:$vid"))
  }

  // ── Lifecycle ───────────────────────────────────────────────────

  def close(): Unit = {
    pool.close()
  }
}

object RedisClient {
  def apply(config: RedisConfig): RedisClient = new RedisClient(config)
}
