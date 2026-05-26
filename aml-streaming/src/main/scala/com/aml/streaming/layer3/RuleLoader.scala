package com.aml.streaming.layer3

import com.aml.common.model.{Condition, Action, RuleDefinition, AlertType, Severity}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.io.File
import scala.collection.JavaConverters._

object RuleLoader {

  def loadRules(yamlPath: String): List[RuleDefinition] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val root = mapper.readTree(new File(yamlPath))
    val rulesNode = root.get("rules")

    rulesNode.elements().asScala.map { ruleNode =>
      val conditions = ruleNode.get("conditions").elements().asScala.map { condNode =>
        Condition(
          field = condNode.get("field").asText(),
          operator = condNode.get("operator").asText(),
          value = parseValue(condNode.get("value"))
        )
      }.toList

      val actions = ruleNode.get("actions").elements().asScala.map { actionNode =>
        Action(
          actionType = actionNode.get("type").asText(),
          template = Option(actionNode.get("template")).map(_.asText()),
          priority = Option(actionNode.get("priority")).map(_.asText())
        )
      }.toList

      RuleDefinition(
        id = ruleNode.get("id").asText(),
        name = ruleNode.get("name").asText(),
        ruleType = AlertType.withName(ruleNode.get("type").asText()),
        severity = Severity.withName(ruleNode.get("severity").asText()),
        window = Option(ruleNode.get("window")).map(_.asText()),
        conditions = conditions,
        actions = actions
      )
    }.toList
  }

  private def parseValue(node: com.fasterxml.jackson.databind.JsonNode): Any = {
    if (node.isArray) {
      node.elements().asScala.map(_.asText()).toList
    } else if (node.isNumber) {
      BigDecimal(node.decimalValue())
    } else {
      node.asText()
    }
  }
}
