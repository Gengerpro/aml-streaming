package com.aml.service.kyc;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface KycRepository extends JpaRepository<KycEntity, String> {
    List<KycEntity> findByRiskLevel(String riskLevel);
    List<KycEntity> findByReviewStatus(String reviewStatus);

    @Query("SELECT k FROM KycEntity k WHERE k.nextReviewDate <= :now AND k.reviewStatus != 'OVERDUE'")
    List<KycEntity> findOverdueReviews(LocalDateTime now);

    @Query("SELECT k FROM KycEntity k WHERE k.riskLevel = 'HIGH' ORDER BY k.riskScore DESC")
    List<KycEntity> findHighRiskCustomers();

    long countByRiskLevel(String riskLevel);
    long countByReviewStatus(String reviewStatus);
}
