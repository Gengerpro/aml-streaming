package com.aml.service.report;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface ReportRepository extends JpaRepository<ReportEntity, String> {
    List<ReportEntity> findByReportType(String reportType);
    List<ReportEntity> findByStatus(String status);
    List<ReportEntity> findByCustomerId(String customerId);
    List<ReportEntity> findByReportTypeAndStatus(String reportType, String status);
}
