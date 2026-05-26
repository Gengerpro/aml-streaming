package com.aml.service.rule;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class RuleService {

    private final RuleVersionRepository ruleVersionRepository;

    public RuleService(RuleVersionRepository ruleVersionRepository) {
        this.ruleVersionRepository = ruleVersionRepository;
    }

    public List<RuleVersionEntity> getAllVersions() {
        return ruleVersionRepository.findByStatusOrderByEffectiveFromDesc("ACTIVE");
    }

    public RuleVersionEntity getEffectiveVersion(LocalDateTime timestamp) {
        return ruleVersionRepository.findEffectiveVersions(timestamp)
            .stream()
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No effective rule version found for: " + timestamp));
    }

    @Transactional
    public RuleVersionEntity createVersion(String rulesJson, LocalDateTime effectiveFrom, String createdBy) {
        RuleVersionEntity entity = new RuleVersionEntity();
        entity.setVersionId(UUID.randomUUID().toString());
        entity.setRulesJson(rulesJson);
        entity.setEffectiveFrom(effectiveFrom);
        entity.setCreatedBy(createdBy);
        entity.setCreatedAt(LocalDateTime.now());
        entity.setStatus("ACTIVE");
        return ruleVersionRepository.save(entity);
    }

    @Transactional
    public RuleVersionEntity deprecateVersion(String versionId) {
        RuleVersionEntity entity = ruleVersionRepository.findById(versionId)
            .orElseThrow(() -> new RuntimeException("Version not found: " + versionId));
        entity.setStatus("DEPRECATED");
        return ruleVersionRepository.save(entity);
    }
}
