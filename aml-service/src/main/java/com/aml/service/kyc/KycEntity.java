package com.aml.service.kyc;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "kyc_profiles")
public class KycEntity {
    @Id
    private String customerId;
    private String name;
    private String idType;          // PASSPORT, NATIONAL_ID, DRIVERS_LICENSE
    private String idNumber;
    private String nationality;
    private String occupation;
    private String incomeSource;
    @Column(columnDefinition = "TEXT")
    private String address;
    private String kycLevel;        // SIMPLIFIED, STANDARD, ENHANCED
    private String riskLevel;       // LOW, MEDIUM, HIGH, PROHIBITED
    private Double riskScore;
    private LocalDateTime kycExpiry;
    private LocalDateTime lastReviewDate;
    private LocalDateTime nextReviewDate;
    private String reviewStatus;    // CURRENT, DUE, OVERDUE
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getIdType() { return idType; }
    public void setIdType(String idType) { this.idType = idType; }
    public String getIdNumber() { return idNumber; }
    public void setIdNumber(String idNumber) { this.idNumber = idNumber; }
    public String getNationality() { return nationality; }
    public void setNationality(String nationality) { this.nationality = nationality; }
    public String getOccupation() { return occupation; }
    public void setOccupation(String occupation) { this.occupation = occupation; }
    public String getIncomeSource() { return incomeSource; }
    public void setIncomeSource(String incomeSource) { this.incomeSource = incomeSource; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public String getKycLevel() { return kycLevel; }
    public void setKycLevel(String kycLevel) { this.kycLevel = kycLevel; }
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    public Double getRiskScore() { return riskScore; }
    public void setRiskScore(Double riskScore) { this.riskScore = riskScore; }
    public LocalDateTime getKycExpiry() { return kycExpiry; }
    public void setKycExpiry(LocalDateTime kycExpiry) { this.kycExpiry = kycExpiry; }
    public LocalDateTime getLastReviewDate() { return lastReviewDate; }
    public void setLastReviewDate(LocalDateTime lastReviewDate) { this.lastReviewDate = lastReviewDate; }
    public LocalDateTime getNextReviewDate() { return nextReviewDate; }
    public void setNextReviewDate(LocalDateTime nextReviewDate) { this.nextReviewDate = nextReviewDate; }
    public String getReviewStatus() { return reviewStatus; }
    public void setReviewStatus(String reviewStatus) { this.reviewStatus = reviewStatus; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
