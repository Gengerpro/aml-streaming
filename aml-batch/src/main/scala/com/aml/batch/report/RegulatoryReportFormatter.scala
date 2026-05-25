package com.aml.batch.report

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object RegulatoryReportFormatter {

  def toFATFXML(reportType: String, data: Map[String, Any]): String = {
    val reportId = data("report_id").toString
    val customerId = data("customer_id").toString
    val amount = data("amount").toString
    val currency = data("currency").toString
    val txnDate = data("txn_date").toString

    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<FATFReport xmlns="urn:fatf:report:1.0">
       |  <ReportHeader>
       |    <ReportType>$reportType</ReportType>
       |    <ReportId>$reportId</ReportId>
       |    <SubmissionDate>${LocalDate.now().format(DateTimeFormatter.ISO_DATE)}</SubmissionDate>
       |  </ReportHeader>
       |  <ReportBody>
       |    <Subject>
       |      <CustomerId>$customerId</CustomerId>
       |    </Subject>
       |    <Transaction>
       |      <Amount currency="$currency">$amount</Amount>
       |      <Date>$txnDate</Date>
       |    </Transaction>
       |  </ReportBody>
       |</FATFReport>""".stripMargin
  }

  def toSummaryJSON(reports: List[Map[String, Any]]): String = {
    val header = s"""{"report_type":"SUMMARY","generated_at":"${LocalDate.now()}","total_count":${reports.size},"reports":["""
    val body = reports.map(r => s"""{"id":"${r("report_id")}","customer":"${r("customer_id")}","amount":${r("amount")}}""").mkString(",")
    header + body + "]}"
  }
}
