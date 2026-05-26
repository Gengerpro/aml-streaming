package com.aml.streaming.integration

import com.aml.common.model.Condition
import com.aml.streaming.layer3.ConditionEvaluator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class E2ESpec extends AnyFlatSpec with Matchers {

  "Rule evaluation pipeline" should "detect CTR for large ATM transaction" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(15000),
      "channel" -> "ATM",
      "txn_count_24h" -> 1,
      "total_amount_24h" -> BigDecimal(15000),
      "max_single_amount" -> BigDecimal(15000)
    )

    val amountCondition = Condition("amount", "GTE", BigDecimal(10000))
    val channelCondition = Condition("channel", "IN", List("ATM", "BRANCH"))

    ConditionEvaluator.evaluate(amountCondition, data) shouldBe true
    ConditionEvaluator.evaluate(channelCondition, data) shouldBe true
  }

  it should "detect SAR for structuring pattern" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(8000),
      "channel" -> "ONLINE",
      "txn_count_24h" -> 6,
      "total_amount_24h" -> BigDecimal(48000),
      "max_single_amount" -> BigDecimal(8000)
    )

    val conditions = List(
      Condition("txn_count_24h", "GTE", 5),
      Condition("max_single_amount", "LT", BigDecimal(10000)),
      Condition("total_amount_24h", "GTE", BigDecimal(40000))
    )

    conditions.foreach { condition =>
      ConditionEvaluator.evaluate(condition, data) shouldBe true
    }
  }

  it should "not trigger false positive for normal transaction" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(500),
      "channel" -> "ONLINE",
      "txn_count_24h" -> 2,
      "total_amount_24h" -> BigDecimal(1000),
      "max_single_amount" -> BigDecimal(500)
    )

    val conditions = List(
      Condition("txn_count_24h", "GTE", 5),
      Condition("max_single_amount", "LT", BigDecimal(10000)),
      Condition("total_amount_24h", "GTE", BigDecimal(40000))
    )

    val allTrue = conditions.forall(c => ConditionEvaluator.evaluate(c, data))
    allTrue shouldBe false
  }
}
