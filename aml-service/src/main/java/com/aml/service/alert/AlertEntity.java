package com.aml.service.alert;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "alert_queue")
public class AlertEntity {
    @Id
    private String alertId;
    private String txnId;
    private String customerId;
    private String alertType;
    private String severity;
    private String ruleId;
    private String ruleDesc;
    private Float score;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String reviewerId;
    private String notes;

    // Getters and setters
    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }
    public String getTxnId() { return txnId; }
    public void setTxnId(String txnId) { this.txnId = txnId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }
    public String getRuleDesc() { return ruleDesc; }
    public void setRuleDesc(String ruleDesc) { this.ruleDesc = ruleDesc; }
    public Float getScore() { return score; }
    public void setScore(Float score) { this.score = score; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    public String getReviewerId() { return reviewerId; }
    public void setReviewerId(String reviewerId) { this.reviewerId = reviewerId; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
}
