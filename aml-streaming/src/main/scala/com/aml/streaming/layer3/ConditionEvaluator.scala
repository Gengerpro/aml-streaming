package com.aml.streaming.layer3

import com.aml.common.model.Condition

object ConditionEvaluator {

  def evaluate(condition: Condition, data: Map[String, Any]): Boolean = {
    val fieldValue = data.get(condition.field)
    fieldValue match {
      case Some(value) => condition.operator match {
        case "GTE" => compareValues(value, condition.value) >= 0
        case "GT"  => compareValues(value, condition.value) > 0
        case "LTE" => compareValues(value, condition.value) <= 0
        case "LT"  => compareValues(value, condition.value) < 0
        case "EQ"  => value.toString == condition.value.toString
        case "NEQ" => value.toString != condition.value.toString
        case "IN"  => condition.value match {
          case list: List[_] => list.map(_.toString).contains(value.toString)
          case _ => false
        }
        case "NOT_IN" => condition.value match {
          case list: List[_] => !list.map(_.toString).contains(value.toString)
          case _ => true
        }
        case _ => false
      }
      case None => false
    }
  }

  private def compareValues(a: Any, b: Any): Int = {
    val numA = BigDecimal(a.toString)
    val numB = BigDecimal(b.toString)
    numA.compare(numB)
  }
}
