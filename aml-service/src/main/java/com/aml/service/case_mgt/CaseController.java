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
    public ResponseEntity<CaseEntity> createCase(@RequestBody Map<String, Object> request) {
        return ResponseEntity.ok(caseService.createCase(
            request.get("customerId").toString(),
            request.get("title").toString(),
            request.getOrDefault("description", "").toString(),
            request.getOrDefault("priority", "MEDIUM").toString(),
            request.getOrDefault("createdBy", "system").toString(),
            request.getOrDefault("linkedAlertIds", "").toString()
        ));
    }

    @PostMapping("/{caseId}/assign")
    public ResponseEntity<CaseEntity> assignCase(
            @PathVariable String caseId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(caseService.assignCase(caseId, request.get("assignee")));
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
    public ResponseEntity<CaseEntity> escalateCase(
            @PathVariable String caseId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(caseService.escalateCase(caseId, request.get("reason")));
    }
}
