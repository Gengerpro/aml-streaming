package com.aml.service.rule;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "rule_versions")
public class RuleVersionEntity {
    @Id
    private String versionId;
    private LocalDateTime effectiveFrom;
    @Column(columnDefinition = "TEXT")
    private String rulesJson;
    private String createdBy;
    private LocalDateTime createdAt;
    private String status;

    public String getVersionId() { return versionId; }
    public void setVersionId(String versionId) { this.versionId = versionId; }
    public LocalDateTime getEffectiveFrom() { return effectiveFrom; }
    public void setEffectiveFrom(LocalDateTime effectiveFrom) { this.effectiveFrom = effectiveFrom; }
    public String getRulesJson() { return rulesJson; }
    public void setRulesJson(String rulesJson) { this.rulesJson = rulesJson; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
