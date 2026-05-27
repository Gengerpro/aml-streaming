package com.aml.service.report;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/reports")
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/{reportId}")
    public ResponseEntity<ReportEntity> getReport(@PathVariable String reportId) {
        return ResponseEntity.ok(reportService.getReport(reportId));
    }

    @GetMapping("/type/{reportType}")
    public ResponseEntity<List<ReportEntity>> getReportsByType(@PathVariable String reportType) {
        return ResponseEntity.ok(reportService.getReportsByType(reportType));
    }

    @GetMapping("/pending")
    public ResponseEntity<List<ReportEntity>> getPendingSubmissions() {
        return ResponseEntity.ok(reportService.getPendingSubmissions());
    }

    @PostMapping("/ctr")
    public ResponseEntity<?> generateCtrReport(@RequestBody Map<String, String> request) {
        String customerId = request.get("customerId");
        String alertId = request.get("alertId");
        if (customerId == null || customerId.isEmpty()) {
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", "customerId is required"));
        }
        if (alertId == null || alertId.isEmpty()) {
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", "alertId is required"));
        }
        return ResponseEntity.ok(reportService.generateCtrReport(
            alertId,
            customerId,
            request.get("amount"),
            request.get("currency"),
            request.get("channel")
        ));
    }

    @PostMapping("/sar")
    public ResponseEntity<?> generateSarReport(@RequestBody Map<String, String> request) {
        String customerId = request.get("customerId");
        String alertId = request.get("alertId");
        if (customerId == null || customerId.isEmpty()) {
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", "customerId is required"));
        }
        if (alertId == null || alertId.isEmpty()) {
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", "alertId is required"));
        }
        return ResponseEntity.ok(reportService.generateSarReport(
            alertId,
            customerId,
            request.get("amount"),
            request.get("currency"),
            request.get("channel"),
            request.get("description")
        ));
    }

    @PostMapping("/{reportId}/submit")
    public ResponseEntity<ReportEntity> submitReport(
            @PathVariable String reportId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(reportService.submitReport(
            reportId,
            request.getOrDefault("submittedBy", "system")
        ));
    }

    @PostMapping("/{reportId}/status")
    public ResponseEntity<?> updateStatus(
            @PathVariable String reportId,
            @RequestBody Map<String, String> request) {
        String status = request.get("status");
        if (status == null || status.isEmpty()) {
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", "status is required"));
        }
        return ResponseEntity.ok(reportService.updateSubmissionStatus(
            reportId,
            status,
            request.get("submissionId")
        ));
    }
}
