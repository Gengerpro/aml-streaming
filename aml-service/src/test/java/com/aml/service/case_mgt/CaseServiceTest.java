package com.aml.service.case_mgt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CaseServiceTest {

    @Mock
    private CaseRepository caseRepository;

    @InjectMocks
    private CaseService caseService;

    @Test
    void createCase_shouldGenerateIdAndSetDefaults() {
        when(caseRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        CaseEntity result = caseService.createCase("CUST-001", "Suspicious activity", "Details", "HIGH", "analyst1", "ALERT-001");

        assertNotNull(result.getCaseId());
        assertEquals("CUST-001", result.getCustomerId());
        assertEquals("OPEN", result.getStatus());
        assertEquals("HIGH", result.getPriority());
        assertNotNull(result.getCreatedAt());
    }

    @Test
    void assignCase_shouldSetStatusToInvestigating() {
        CaseEntity existing = new CaseEntity();
        existing.setCaseId("CASE-001");
        existing.setStatus("OPEN");
        when(caseRepository.findById("CASE-001")).thenReturn(Optional.of(existing));
        when(caseRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        CaseEntity result = caseService.assignCase("CASE-001", "analyst2");

        assertEquals("INVESTIGATING", result.getStatus());
        assertEquals("analyst2", result.getAssignedTo());
    }

    @Test
    void updateCase_withResolution_shouldCloseCase() {
        CaseEntity existing = new CaseEntity();
        existing.setCaseId("CASE-001");
        existing.setStatus("INVESTIGATING");
        when(caseRepository.findById("CASE-001")).thenReturn(Optional.of(existing));
        when(caseRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        CaseEntity result = caseService.updateCase("CASE-001", null, null, "CONFIRMED_SAR");

        assertEquals("CLOSED", result.getStatus());
        assertEquals("CONFIRMED_SAR", result.getResolution());
        assertNotNull(result.getClosedAt());
    }

    @Test
    void escalateCase_shouldSetStatusEscalated() {
        CaseEntity existing = new CaseEntity();
        existing.setCaseId("CASE-001");
        existing.setStatus("INVESTIGATING");
        when(caseRepository.findById("CASE-001")).thenReturn(Optional.of(existing));
        when(caseRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        CaseEntity result = caseService.escalateCase("CASE-001", "Complex pattern detected");

        assertEquals("ESCALATED", result.getStatus());
        assertTrue(result.getNotes().contains("Complex pattern detected"));
    }
}
