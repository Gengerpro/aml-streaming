package com.aml.service.kyc;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kyc")
public class KycController {

    private final KycService kycService;

    public KycController(KycService kycService) {
        this.kycService = kycService;
    }

    @GetMapping("/{customerId}")
    public ResponseEntity<KycEntity> getProfile(@PathVariable String customerId) {
        return ResponseEntity.ok(kycService.getProfile(customerId));
    }

    @GetMapping("/high-risk")
    public ResponseEntity<List<KycEntity>> getHighRiskCustomers() {
        return ResponseEntity.ok(kycService.getHighRiskCustomers());
    }

    @GetMapping("/overdue-reviews")
    public ResponseEntity<List<KycEntity>> getOverdueReviews() {
        return ResponseEntity.ok(kycService.getOverdueReviews());
    }

    @PostMapping
    public ResponseEntity<KycEntity> createOrUpdateProfile(@RequestBody KycEntity profile) {
        return ResponseEntity.ok(kycService.createOrUpdateProfile(profile));
    }

    @PostMapping("/{customerId}/risk")
    public ResponseEntity<KycEntity> updateRiskLevel(
            @PathVariable String customerId,
            @RequestBody Map<String, Object> request) {
        return ResponseEntity.ok(kycService.updateRiskLevel(
            customerId,
            request.get("riskLevel").toString(),
            Double.parseDouble(request.get("riskScore").toString())
        ));
    }

    @PostMapping("/{customerId}/review")
    public ResponseEntity<KycEntity> completeReview(
            @PathVariable String customerId,
            @RequestBody Map<String, String> request) {
        return ResponseEntity.ok(kycService.completeReview(
            customerId,
            request.getOrDefault("notes", "")
        ));
    }
}
