package com.aml.streaming.layer3

import com.aml.common.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RuleEngineSpec extends AnyFlatSpec with Matchers {

  val rulesPath = getClass.getResource("/rules/test-rules.yaml").getPath
  val ruleEngine = RuleEngine.loadFromYaml(rulesPath)

  "RuleLoader" should "load rules from YAML" in {
    val rules = RuleLoader.loadRules(rulesPath)
    rules.size shouldBe 2
    rules.head.id shouldBe "CTR-001"
    rules(1).id shouldBe "SAR-001"
  }

  "ConditionEvaluator" should "evaluate GTE condition" in {
    val condition = Condition("amount", "GTE", BigDecimal(10000))
    val data = Map("amount" -> BigDecimal(15000))
    ConditionEvaluator.evaluate(condition, data) shouldBe true
  }

  it should "evaluate LT condition" in {
    val condition = Condition("amount", "LT", BigDecimal(10000))
    val data = Map("amount" -> BigDecimal(5000))
    ConditionEvaluator.evaluate(condition, data) shouldBe true
  }

  it should "evaluate IN condition" in {
    val condition = Condition("channel", "IN", List("ATM", "BRANCH"))
    val data = Map("channel" -> "ATM")
    ConditionEvaluator.evaluate(condition, data) shouldBe true
  }

  it should "evaluate EQ condition" in {
    val condition = Condition("currency", "EQ", "USD")
    val data = Map("currency" -> "USD")
    ConditionEvaluator.evaluate(condition, data) shouldBe true
  }

  "RuleEngine" should "match CTR rule for large cash transaction" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(15000),
      "channel" -> "ATM",
      "txn_count_24h" -> 1L,
      "total_amount_24h" -> BigDecimal(15000),
      "max_single_amount" -> BigDecimal(15000)
    )
    val matches = ruleEngine.evaluate(data)
    matches.map(_.id) should contain("CTR-001")
  }

  it should "match SAR-001 rule for structuring" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(8000),
      "channel" -> "ONLINE",
      "txn_count_24h" -> 6L,
      "total_amount_24h" -> BigDecimal(48000),
      "max_single_amount" -> BigDecimal(8000)
    )
    val matches = ruleEngine.evaluate(data)
    matches.map(_.id) should contain("SAR-001")
  }

  it should "not match when conditions are not met" in {
    val data = Map[String, Any](
      "amount" -> BigDecimal(100),
      "channel" -> "ONLINE",
      "txn_count_24h" -> 1L,
      "total_amount_24h" -> BigDecimal(100),
      "max_single_amount" -> BigDecimal(100)
    )
    val matches = ruleEngine.evaluate(data)
    matches shouldBe empty
  }
}
