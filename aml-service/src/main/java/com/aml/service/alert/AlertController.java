package com.aml.service.alert;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertService alertService;

    public AlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    @GetMapping("/new")
    public ResponseEntity<List<AlertEntity>> getNewAlerts() {
        return ResponseEntity.ok(alertService.getNewAlerts());
    }

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<AlertEntity>> getAlertsByCustomer(@PathVariable String customerId) {
        return ResponseEntity.ok(alertService.getAlertsByCustomer(customerId));
    }

    @PostMapping("/{alertId}/review")
    public ResponseEntity<AlertEntity> reviewAlert(
            @PathVariable String alertId,
            @RequestBody Map<String, String> request) {
        String reviewerId = request.get("reviewerId");
        String action = request.get("action");
        String notes = request.get("notes");
        return ResponseEntity.ok(alertService.reviewAlert(alertId, reviewerId, action, notes));
    }

    @PostMapping("/{alertId}/report")
    public ResponseEntity<AlertEntity> reportAlert(@PathVariable String alertId) {
        return ResponseEntity.ok(alertService.reportAlert(alertId));
    }
}
