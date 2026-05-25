package com.aml.common.model

case class RuleDefinition(
  id: String,
  name: String,
  ruleType: AlertType.Value,
  severity: Severity.Value,
  window: Option[String],
  conditions: List[Condition],
  actions: List[Action]
)

case class Condition(
  field: String,
  operator: String,
  value: Any
)

case class Action(
  actionType: String,
  template: Option[String] = None,
  priority: Option[String] = None
)

case class RuleVersion(
  versionId: String,
  effectiveFrom: Long,
  rules: List[RuleDefinition],
  createdBy: String,
  createdAt: Long,
  status: String
)
