package com.aml.streaming.layer4

import java.time.Instant
import java.time.temporal.ChronoUnit

case class AlertInfo(
  alertId: String,
  customerId: String,
  ruleId: String,
  severity: String,
  createdAt: Instant
)

object AlertDeduplicator {

  private val severityOrder = Map("LOW" -> 0, "MEDIUM" -> 1, "HIGH" -> 2, "CRITICAL" -> 3)

  def deduplicate(alerts: List[AlertInfo], windowMinutes: Int): List[AlertInfo] = {
    val grouped = alerts.groupBy(a => (a.customerId, a.ruleId))

    grouped.values.toList.flatMap { group =>
      val sorted = group.sortBy(_.createdAt)
      mergeWithinWindow(sorted, windowMinutes)
    }
  }

  private def mergeWithinWindow(alerts: List[AlertInfo], windowMinutes: Int): List[AlertInfo] = {
    if (alerts.isEmpty) return List.empty

    var result = List(alerts.head)
    for (alert <- alerts.tail) {
      val last = result.last
      val minutesBetween = ChronoUnit.MINUTES.between(last.createdAt, alert.createdAt)

      if (minutesBetween <= windowMinutes && last.customerId == alert.customerId && last.ruleId == alert.ruleId) {
        val higherSeverity = if (severityOrder.getOrElse(alert.severity, 0) > severityOrder.getOrElse(last.severity, 0))
          alert.severity else last.severity
        result = result.init :+ last.copy(severity = higherSeverity, alertId = last.alertId)
      } else {
        result = result :+ alert
      }
    }
    result
  }
}
