package com.aml.service.report;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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
    public ResponseEntity<ReportEntity> generateCtrReport(@RequestBody Map<String, String> request) {
        return ResponseEntity.ok(reportService.generateCtrReport(
            request.get("alertId"),
            request.get("customerId"),
            request.get("amount"),
            request.get("currency"),
            request.get("channel")
        ));
    }

    @PostMapping("/sar")
    public ResponseEntity<ReportEntity> generateSarReport(@RequestBody Map<String, String> request) {
        return ResponseEntity.ok(reportService.generateSarReport(
            request.get("alertId"),
            request.get("customerId"),
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
    public ResponseEntity<ReportEntity> updateStatus(
            @PathVariable String reportId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(reportService.updateSubmissionStatus(
            reportId,
            request.get("status"),
            request.get("submissionId")
        ));
    }
}
