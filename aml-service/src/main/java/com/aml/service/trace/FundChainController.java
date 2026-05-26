package com.aml.service.trace;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/trace")
public class FundChainController {

    private final FundChainService fundChainService;

    public FundChainController(FundChainService fundChainService) {
        this.fundChainService = fundChainService;
    }

    @GetMapping("/forward/{customerId}")
    public ResponseEntity<List<List<Map<String, Object>>>> traceForward(
            @PathVariable String customerId,
            @RequestParam(defaultValue = "5") int maxHops) {
        return ResponseEntity.ok(fundChainService.traceForward(customerId, maxHops));
    }

    @GetMapping("/backward/{customerId}")
    public ResponseEntity<List<List<Map<String, Object>>>> traceBackward(
            @PathVariable String customerId,
            @RequestParam(defaultValue = "5") int maxHops) {
        return ResponseEntity.ok(fundChainService.traceBackward(customerId, maxHops));
    }
}
