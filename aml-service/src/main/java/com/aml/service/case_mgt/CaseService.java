package com.aml.service.case_mgt;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class CaseService {

    private final CaseRepository caseRepository;

    public CaseService(CaseRepository caseRepository) {
        this.caseRepository = caseRepository;
    }

    public List<CaseEntity> getOpenCases() {
        return caseRepository.findOpenCasesOrderedByPriority();
    }

    public List<CaseEntity> getCasesByCustomer(String customerId) {
        return caseRepository.findByCustomerId(customerId);
    }

    public List<CaseEntity> getCasesByAssignee(String assignee) {
        return caseRepository.findByAssignedTo(assignee);
    }

    public CaseEntity getCase(String caseId) {
        return caseRepository.findById(caseId)
            .orElseThrow(() -> new RuntimeException("Case not found: " + caseId));
    }

    @Transactional
    public CaseEntity createCase(String customerId, String title, String description,
                                  String priority, String createdBy, String linkedAlertIds) {
        CaseEntity entity = new CaseEntity();
        entity.setCaseId(UUID.randomUUID().toString());
        entity.setCustomerId(customerId);
        entity.setTitle(title);
        entity.setDescription(description);
        entity.setPriority(priority);
        entity.setStatus("OPEN");
        entity.setCreatedBy(createdBy);
        entity.setLinkedAlertIds(linkedAlertIds);
        entity.setCreatedAt(LocalDateTime.now());
        entity.setUpdatedAt(LocalDateTime.now());
        return caseRepository.save(entity);
    }

    @Transactional
    public CaseEntity assignCase(String caseId, String assignee) {
        CaseEntity entity = getCase(caseId);
        entity.setAssignedTo(assignee);
        entity.setStatus("INVESTIGATING");
        entity.setUpdatedAt(LocalDateTime.now());
        return caseRepository.save(entity);
    }

    @Transactional
    public CaseEntity updateCase(String caseId, String status, String notes, String resolution) {
        CaseEntity entity = getCase(caseId);
        if (status != null) entity.setStatus(status);
        if (notes != null) entity.setNotes(notes);
        if (resolution != null) {
            entity.setResolution(resolution);
            entity.setStatus("CLOSED");
            entity.setClosedAt(LocalDateTime.now());
        }
        entity.setUpdatedAt(LocalDateTime.now());
        return caseRepository.save(entity);
    }

    @Transactional
    public CaseEntity escalateCase(String caseId, String reason) {
        CaseEntity entity = getCase(caseId);
        entity.setStatus("ESCALATED");
        entity.setNotes("ESCALATED: " + reason);
        entity.setUpdatedAt(LocalDateTime.now());
        return caseRepository.save(entity);
    }
}
