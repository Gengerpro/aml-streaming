package com.aml.service.alert;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertServiceTest {

    @Mock
    private AlertRepository alertRepository;

    @InjectMocks
    private AlertService alertService;

    private AlertEntity createTestAlert(String id, String severity, String status) {
        AlertEntity alert = new AlertEntity();
        alert.setAlertId(id);
        alert.setCustomerId("CUST-001");
        alert.setAlertType("SAR");
        alert.setSeverity(severity);
        alert.setRuleId("SAR-001");
        alert.setRuleDesc("Test alert");
        alert.setScore(0.8f);
        alert.setStatus(status);
        alert.setCreatedAt(LocalDateTime.now());
        alert.setUpdatedAt(LocalDateTime.now());
        return alert;
    }

    @Test
    void getNewAlerts_shouldDelegateToRepository() {
        List<AlertEntity> expected = Arrays.asList(
            createTestAlert("A1", "CRITICAL", "NEW"),
            createTestAlert("A2", "HIGH", "NEW")
        );
        when(alertRepository.findNewAlertsOrderedByPriority()).thenReturn(expected);

        List<AlertEntity> result = alertService.getNewAlerts();

        assertEquals(2, result.size());
        assertEquals("CRITICAL", result.get(0).getSeverity());
        verify(alertRepository).findNewAlertsOrderedByPriority();
    }

    @Test
    void reviewAlert_withConfirm_shouldSetEscalated() {
        AlertEntity alert = createTestAlert("A1", "HIGH", "NEW");
        when(alertRepository.findById("A1")).thenReturn(Optional.of(alert));
        when(alertRepository.save(any())).thenReturn(alert);

        AlertEntity result = alertService.reviewAlert("A1", "analyst1", "CONFIRM", "Confirmed suspicious");

        assertEquals("ESCALATED", result.getStatus());
        assertEquals("analyst1", result.getReviewerId());
        verify(alertRepository).save(alert);
    }

    @Test
    void reviewAlert_withClose_shouldSetClosed() {
        AlertEntity alert = createTestAlert("A1", "MEDIUM", "NEW");
        when(alertRepository.findById("A1")).thenReturn(Optional.of(alert));
        when(alertRepository.save(any())).thenReturn(alert);

        AlertEntity result = alertService.reviewAlert("A1", "analyst1", "CLOSE", "False positive");

        assertEquals("CLOSED", result.getStatus());
    }

    @Test
    void reviewAlert_withUnknownAction_shouldThrow() {
        AlertEntity alert = createTestAlert("A1", "HIGH", "NEW");
        when(alertRepository.findById("A1")).thenReturn(Optional.of(alert));

        assertThrows(IllegalArgumentException.class,
            () -> alertService.reviewAlert("A1", "analyst1", "INVALID", ""));
    }

    @Test
    void reviewAlert_withNonexistentAlert_shouldThrow() {
        when(alertRepository.findById("MISSING")).thenReturn(Optional.empty());

        assertThrows(RuntimeException.class,
            () -> alertService.reviewAlert("MISSING", "analyst1", "CLOSE", ""));
    }

    @Test
    void reportAlert_shouldSetReportedStatus() {
        AlertEntity alert = createTestAlert("A1", "HIGH", "ESCALATED");
        when(alertRepository.findById("A1")).thenReturn(Optional.of(alert));
        when(alertRepository.save(any())).thenReturn(alert);

        AlertEntity result = alertService.reportAlert("A1");

        assertEquals("REPORTED", result.getStatus());
    }

    @Test
    void getAlertsByCustomer_shouldDelegate() {
        when(alertRepository.findByCustomerId("CUST-001")).thenReturn(Arrays.asList(
            createTestAlert("A1", "HIGH", "NEW")
        ));

        List<AlertEntity> result = alertService.getAlertsByCustomer("CUST-001");

        assertEquals(1, result.size());
        assertEquals("CUST-001", result.get(0).getCustomerId());
    }
}
