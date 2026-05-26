package com.aml.service.case_mgt;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cases")
public class CaseController {

    private final CaseService caseService;

    public CaseController(CaseService caseService) {
        this.caseService = caseService;
    }

    @GetMapping("/open")
    public ResponseEntity<List<CaseEntity>> getOpenCases() {
        return ResponseEntity.ok(caseService.getOpenCases());
    }

    @GetMapping("/{caseId}")
    public ResponseEntity<CaseEntity> getCase(@PathVariable String caseId) {
        return ResponseEntity.ok(caseService.getCase(caseId));
    }

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<CaseEntity>> getCasesByCustomer(@PathVariable String customerId) {
        return ResponseEntity.ok(caseService.getCasesByCustomer(customerId));
    }

    @GetMapping("/assignee/{assignee}")
    public ResponseEntity<List<CaseEntity>> getCasesByAssignee(@PathVariable String assignee) {
        return ResponseEntity.ok(caseService.getCasesByAssignee(assignee));
    }

    @PostMapping
    public ResponseEntity<?> createCase(@RequestBody Map<String, Object> request) {
        String customerId = request.get("customerId") != null ? request.get("customerId").toString() : null;
        String title = request.get("title") != null ? request.get("title").toString() : null;
        if (customerId == null || customerId.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "customerId is required"));
        }
        if (title == null || title.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "title is required"));
        }
        return ResponseEntity.ok(caseService.createCase(
            customerId,
            title,
            request.getOrDefault("description", "").toString(),
            request.getOrDefault("priority", "MEDIUM").toString(),
            request.getOrDefault("createdBy", "system").toString(),
            request.getOrDefault("linkedAlertIds", "").toString()
        ));
    }

    @PostMapping("/{caseId}/assign")
    public ResponseEntity<?> assignCase(
            @PathVariable String caseId,
            @RequestBody Map<String, String> request) {
        String assignee = request.get("assignee");
        if (assignee == null || assignee.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "assignee is required"));
        }
        return ResponseEntity.ok(caseService.assignCase(caseId, assignee));
    }

    @PostMapping("/{caseId}/update")
    public ResponseEntity<CaseEntity> updateCase(
            @PathVariable String caseId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(caseService.updateCase(
            caseId,
            request.get("status"),
            request.get("notes"),
            request.get("resolution")
        ));
    }

    @PostMapping("/{caseId}/escalate")
    public ResponseEntity<?> escalateCase(
            @PathVariable String caseId,
            @RequestBody Map<String, String> request) {
        String reason = request.get("reason");
        if (reason == null || reason.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "reason is required"));
        }
        return ResponseEntity.ok(caseService.escalateCase(caseId, reason));
    }
}
