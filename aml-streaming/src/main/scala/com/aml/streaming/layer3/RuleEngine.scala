package com.aml.streaming.layer3

import com.aml.common.model.{Condition, RuleDefinition, Severity}

case class RuleMatch(
  id: String,
  name: String,
  ruleType: String,
  severity: String,
  score: Float,
  message: String
)

class RuleEngine(rules: List[RuleDefinition]) {

  def evaluate(data: Map[String, Any]): List[RuleMatch] = {
    rules.filter(rule => allConditionsMet(rule.conditions, data))
      .map { rule =>
        val score = calculateScore(rule, data)
        RuleMatch(
          id = rule.id,
          name = rule.name,
          ruleType = rule.ruleType.toString,
          severity = rule.severity.toString,
          score = score,
          message = formatMessage(rule, data)
        )
      }
  }

  private def allConditionsMet(conditions: List[Condition], data: Map[String, Any]): Boolean = {
    conditions.forall(cond => ConditionEvaluator.evaluate(cond, data))
  }

  private def calculateScore(rule: RuleDefinition, data: Map[String, Any]): Float = {
    rule.severity match {
      case Severity.CRITICAL => 1.0f
      case Severity.HIGH     => 0.8f
      case Severity.MEDIUM   => 0.5f
      case Severity.LOW      => 0.3f
    }
  }

  private def formatMessage(rule: RuleDefinition, data: Map[String, Any]): String = {
    rule.actions.headOption.flatMap(_.template) match {
      case Some(template) =>
        data.foldLeft(template) { case (msg, (key, value)) =>
          msg.replace(s"$${$key}", value.toString)
        }
      case None => s"Rule ${rule.id} triggered"
    }
  }
}

object RuleEngine {
  def loadFromYaml(yamlPath: String): RuleEngine = {
    val rules = RuleLoader.loadRules(yamlPath)
    new RuleEngine(rules)
  }
}
