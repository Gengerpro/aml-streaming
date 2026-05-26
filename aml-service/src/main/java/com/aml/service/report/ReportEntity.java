package com.aml.service.report;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "regulatory_reports")
public class ReportEntity {
    @Id
    private String reportId;
    private String reportType;      // CTR, SAR, SUMMARY
    private String relatedAlertId;
    private String customerId;
    private String format;          // FATF_XML, JSON, PDF
    @Column(columnDefinition = "TEXT")
    private String content;
    private String status;          // DRAFT, PENDING_SUBMISSION, SUBMITTED, ACCEPTED, REJECTED
    private String submissionId;    // External regulatory reference
    private LocalDateTime generatedAt;
    private LocalDateTime submittedAt;
    private String submittedBy;
    private LocalDateTime createdAt;

    public String getReportId() { return reportId; }
    public void setReportId(String reportId) { this.reportId = reportId; }
    public String getReportType() { return reportType; }
    public void setReportType(String reportType) { this.reportType = reportType; }
    public String getRelatedAlertId() { return relatedAlertId; }
    public void setRelatedAlertId(String relatedAlertId) { this.relatedAlertId = relatedAlertId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getSubmissionId() { return submissionId; }
    public void setSubmissionId(String submissionId) { this.submissionId = submissionId; }
    public LocalDateTime getGeneratedAt() { return generatedAt; }
    public void setGeneratedAt(LocalDateTime generatedAt) { this.generatedAt = generatedAt; }
    public LocalDateTime getSubmittedAt() { return submittedAt; }
    public void setSubmittedAt(LocalDateTime submittedAt) { this.submittedAt = submittedAt; }
    public String getSubmittedBy() { return submittedBy; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}
