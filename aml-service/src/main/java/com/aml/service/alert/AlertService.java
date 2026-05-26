package com.aml.service.alert;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class AlertService {

    private final AlertRepository alertRepository;

    public AlertService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    public List<AlertEntity> getNewAlerts() {
        return alertRepository.findNewAlertsOrderedByPriority();
    }

    public List<AlertEntity> getAlertsByCustomer(String customerId) {
        return alertRepository.findByCustomerId(customerId);
    }

    @Transactional
    public AlertEntity reviewAlert(String alertId, String reviewerId, String action, String notes) {
        AlertEntity alert = alertRepository.findById(alertId)
            .orElseThrow(() -> new RuntimeException("Alert not found: " + alertId));

        alert.setReviewerId(reviewerId);
        alert.setNotes(notes);
        alert.setUpdatedAt(LocalDateTime.now());

        switch (action) {
            case "CONFIRM":
                alert.setStatus("ESCALATED");
                break;
            case "CLOSE":
                alert.setStatus("CLOSED");
                break;
            case "ESCALATE":
                alert.setStatus("ESCALATED");
                break;
            default:
                throw new IllegalArgumentException("Unknown action: " + action);
        }

        return alertRepository.save(alert);
    }

    @Transactional
    public AlertEntity reportAlert(String alertId) {
        AlertEntity alert = alertRepository.findById(alertId)
            .orElseThrow(() -> new RuntimeException("Alert not found: " + alertId));
        alert.setStatus("REPORTED");
        alert.setUpdatedAt(LocalDateTime.now());
        return alertRepository.save(alert);
    }
}
