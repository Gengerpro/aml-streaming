package com.aml.service.kyc;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class KycService {

    private final KycRepository kycRepository;

    public KycService(KycRepository kycRepository) {
        this.kycRepository = kycRepository;
    }

    public KycEntity getProfile(String customerId) {
        return kycRepository.findById(customerId)
            .orElseThrow(() -> new RuntimeException("KYC profile not found: " + customerId));
    }

    public List<KycEntity> getHighRiskCustomers() {
        return kycRepository.findHighRiskCustomers();
    }

    public List<KycEntity> getOverdueReviews() {
        return kycRepository.findOverdueReviews(LocalDateTime.now());
    }

    @Transactional
    public KycEntity createOrUpdateProfile(KycEntity profile) {
        profile.setUpdatedAt(LocalDateTime.now());
        if (profile.getCreatedAt() == null) {
            profile.setCreatedAt(LocalDateTime.now());
        }
        // Auto-calculate next review date based on risk level
        if (profile.getNextReviewDate() == null) {
            profile.setNextReviewDate(calculateNextReview(profile.getRiskLevel()));
        }
        profile.setReviewStatus(calculateReviewStatus(profile.getNextReviewDate()));
        return kycRepository.save(profile);
    }

    @Transactional
    public KycEntity updateRiskLevel(String customerId, String riskLevel, Double riskScore) {
        KycEntity entity = getProfile(customerId);
        entity.setRiskLevel(riskLevel);
        entity.setRiskScore(riskScore);
        entity.setNextReviewDate(calculateNextReview(riskLevel));
        entity.setReviewStatus(calculateReviewStatus(entity.getNextReviewDate()));
        entity.setUpdatedAt(LocalDateTime.now());
        return kycRepository.save(entity);
    }

    @Transactional
    public KycEntity completeReview(String customerId, String reviewerNotes) {
        KycEntity entity = getProfile(customerId);
        entity.setLastReviewDate(LocalDateTime.now());
        entity.setNextReviewDate(calculateNextReview(entity.getRiskLevel()));
        entity.setReviewStatus("CURRENT");
        entity.setUpdatedAt(LocalDateTime.now());
        return kycRepository.save(entity);
    }

    /**
     * Review schedule per design spec:
     * HIGH: quarterly, MEDIUM: annually, LOW: every 2 years
     */
    private LocalDateTime calculateNextReview(String riskLevel) {
        LocalDateTime now = LocalDateTime.now();
        if (riskLevel == null) return now.plusYears(1);
        switch (riskLevel) {
            case "HIGH":
            case "PROHIBITED":
                return now.plusMonths(3);
            case "MEDIUM":
                return now.plusYears(1);
            default:
                return now.plusYears(2);
        }
    }

    private String calculateReviewStatus(LocalDateTime nextReview) {
        if (nextReview == null) return "DUE";
        LocalDateTime now = LocalDateTime.now();
        if (now.isAfter(nextReview)) return "OVERDUE";
        if (now.plusDays(30).isAfter(nextReview)) return "DUE";
        return "CURRENT";
    }
}
