package com.aml.service.rule;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/rules")
public class RuleController {

    private final RuleService ruleService;

    public RuleController(RuleService ruleService) {
        this.ruleService = ruleService;
    }

    @GetMapping("/versions")
    public ResponseEntity<List<RuleVersionEntity>> getAllVersions() {
        return ResponseEntity.ok(ruleService.getAllVersions());
    }

    @GetMapping("/effective")
    public ResponseEntity<RuleVersionEntity> getEffectiveVersion(@RequestParam String timestamp) {
        return ResponseEntity.ok(ruleService.getEffectiveVersion(LocalDateTime.parse(timestamp)));
    }

    @PostMapping("/versions")
    public ResponseEntity<RuleVersionEntity> createVersion(@RequestBody Map<String, Object> request) {
        String rulesJson = request.get("rulesJson").toString();
        LocalDateTime effectiveFrom = LocalDateTime.parse(request.get("effectiveFrom").toString());
        String createdBy = request.get("createdBy").toString();
        return ResponseEntity.ok(ruleService.createVersion(rulesJson, effectiveFrom, createdBy));
    }

    @PostMapping("/versions/{versionId}/deprecate")
    public ResponseEntity<RuleVersionEntity> deprecateVersion(@PathVariable String versionId) {
        return ResponseEntity.ok(ruleService.deprecateVersion(versionId));
    }
}
