package com.aml.service.rule;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface RuleVersionRepository extends JpaRepository<RuleVersionEntity, String> {
    @Query("SELECT r FROM RuleVersionEntity r WHERE r.effectiveFrom <= :timestamp AND r.status = 'ACTIVE' ORDER BY r.effectiveFrom DESC")
    List<RuleVersionEntity> findEffectiveVersions(LocalDateTime timestamp);

    List<RuleVersionEntity> findByStatusOrderByEffectiveFromDesc(String status);
}
