package com.aml.service.sla;

import com.aml.service.alert.AlertEntity;
import com.aml.service.alert.AlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * SLA Timer that monitors alert age and auto-escalates based on severity.
 *
 * Per design spec:
 *   CRITICAL: review 30min, report 2h, auto-escalate on timeout
 *   HIGH:     review 2h,   report 6h, auto-escalate on timeout
 *   MEDIUM:   review 8h,   report 24h, reminder on timeout
 *   LOW:      review 24h,  report T+1, no auto-escalation
 */
@Component
public class SlaScheduler {

    private static final Logger log = LoggerFactory.getLogger(SlaScheduler.class);

    private final AlertRepository alertRepository;

    public SlaScheduler(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    // Run every 60 seconds
    @Scheduled(fixedRate = 60000)
    @Transactional
    public void checkSlaCompliance() {
        List<AlertEntity> newAlerts = alertRepository.findByStatus("NEW");
        LocalDateTime now = LocalDateTime.now();

        for (AlertEntity alert : newAlerts) {
            if (alert.getCreatedAt() == null) continue;

            long minutesSinceCreation = Duration.between(alert.getCreatedAt(), now).toMinutes();
            String severity = alert.getSeverity();

            if (shouldEscalate(severity, minutesSinceCreation)) {
                escalateAlert(alert, severity, minutesSinceCreation);
            } else if (shouldRemind(severity, minutesSinceCreation)) {
                remindAlert(alert, severity, minutesSinceCreation);
            }
        }
    }

    /**
     * Auto-escalation rules:
     * - CRITICAL: escalate after 30 minutes
     * - HIGH: escalate after 2 hours (120 min)
     * - MEDIUM: no auto-escalate, only reminder
     * - LOW: no action
     */
    private boolean shouldEscalate(String severity, long minutes) {
        if (severity == null) return false;
        switch (severity) {
            case "CRITICAL": return minutes > 30;
            case "HIGH":     return minutes > 120;
            default:         return false;
        }
    }

    private boolean shouldRemind(String severity, long minutes) {
        if (severity == null) return false;
        switch (severity) {
            case "CRITICAL": return minutes > 15;  // reminder at 15min
            case "HIGH":     return minutes > 60;   // reminder at 1h
            case "MEDIUM":   return minutes > 480;  // reminder at 8h
            default:         return false;
        }
    }

    private void escalateAlert(AlertEntity alert, String severity, long minutes) {
        log.warn("SLA BREACH: Alert {} ({}) is {} minutes old, auto-escalating",
                 alert.getAlertId(), severity, minutes);
        alert.setStatus("ESCALATED");
        alert.setNotes(String.format("Auto-escalated by SLA timer: %s alert overdue by %d minutes",
                                      severity, minutes - getSlaThresholdMinutes(severity)));
        alert.setUpdatedAt(LocalDateTime.now());
        alertRepository.save(alert);
    }

    private void remindAlert(AlertEntity alert, String severity, long minutes) {
        log.info("SLA REMINDER: Alert {} ({}) is {} minutes old, approaching SLA deadline",
                 alert.getAlertId(), severity, minutes);
        // In production: send notification email/webhook
        // For now, just log the reminder
    }

    private long getSlaThresholdMinutes(String severity) {
        if (severity == null) return 1440;
        switch (severity) {
            case "CRITICAL": return 30;
            case "HIGH":     return 120;
            case "MEDIUM":   return 480;
            default:         return 1440;
        }
    }
}
