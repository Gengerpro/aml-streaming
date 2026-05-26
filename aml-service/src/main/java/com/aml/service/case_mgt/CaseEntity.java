package com.aml.service.case_mgt;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "cases")
public class CaseEntity {
    @Id
    private String caseId;
    private String customerId;
    private String title;
    private String description;
    private String priority;       // LOW, MEDIUM, HIGH, CRITICAL
    private String status;         // OPEN, INVESTIGATING, ESCALATED, CLOSED
    private String assignedTo;
    private String createdBy;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime closedAt;
    private String resolution;     // CONFIRMED_SAR, FALSE_POSITIVE, INCONCLUSIVE
    private String notes;

    // Linked alert IDs (comma-separated for simplicity)
    @Column(columnDefinition = "TEXT")
    private String linkedAlertIds;

    public String getCaseId() { return caseId; }
    public void setCaseId(String caseId) { this.caseId = caseId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getAssignedTo() { return assignedTo; }
    public void setAssignedTo(String assignedTo) { this.assignedTo = assignedTo; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    public LocalDateTime getClosedAt() { return closedAt; }
    public void setClosedAt(LocalDateTime closedAt) { this.closedAt = closedAt; }
    public String getResolution() { return resolution; }
    public void setResolution(String resolution) { this.resolution = resolution; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
    public String getLinkedAlertIds() { return linkedAlertIds; }
    public void setLinkedAlertIds(String linkedAlertIds) { this.linkedAlertIds = linkedAlertIds; }
}
