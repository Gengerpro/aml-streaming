package com.aml.service.report;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

@Service
public class ReportService {

    private final ReportRepository reportRepository;

    public ReportService(ReportRepository reportRepository) {
        this.reportRepository = reportRepository;
    }

    public ReportEntity getReport(String reportId) {
        return reportRepository.findById(reportId)
            .orElseThrow(() -> new RuntimeException("Report not found: " + reportId));
    }

    public List<ReportEntity> getReportsByType(String reportType) {
        return reportRepository.findByReportType(reportType);
    }

    public List<ReportEntity> getPendingSubmissions() {
        return reportRepository.findByStatus("PENDING_SUBMISSION");
    }

    /**
     * Generate CTR report in FATF XML format.
     * CTR is auto-generated for large cash transactions.
     */
    @Transactional
    public ReportEntity generateCtrReport(String alertId, String customerId,
                                           String amount, String currency, String channel) {
        String reportId = "CTR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String xml = buildFatfXml("CTR", reportId, customerId, amount, currency, channel);

        ReportEntity entity = new ReportEntity();
        entity.setReportId(reportId);
        entity.setReportType("CTR");
        entity.setRelatedAlertId(alertId);
        entity.setCustomerId(customerId);
        entity.setFormat("FATF_XML");
        entity.setContent(xml);
        entity.setStatus("PENDING_SUBMISSION");
        entity.setGeneratedAt(LocalDateTime.now());
        entity.setCreatedAt(LocalDateTime.now());
        return reportRepository.save(entity);
    }

    /**
     * Generate SAR report after manual confirmation.
     */
    @Transactional
    public ReportEntity generateSarReport(String alertId, String customerId,
                                           String amount, String currency,
                                           String channel, String description) {
        String reportId = "SAR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String xml = buildFatfXml("SAR", reportId, customerId, amount, currency, channel);

        ReportEntity entity = new ReportEntity();
        entity.setReportId(reportId);
        entity.setReportType("SAR");
        entity.setRelatedAlertId(alertId);
        entity.setCustomerId(customerId);
        entity.setFormat("FATF_XML");
        entity.setContent(xml);
        entity.setStatus("PENDING_SUBMISSION");
        entity.setGeneratedAt(LocalDateTime.now());
        entity.setCreatedAt(LocalDateTime.now());
        return reportRepository.save(entity);
    }

    /**
     * Submit report to regulatory authority.
     */
    @Transactional
    public ReportEntity submitReport(String reportId, String submittedBy) {
        ReportEntity entity = getReport(reportId);
        entity.setStatus("SUBMITTED");
        entity.setSubmittedAt(LocalDateTime.now());
        entity.setSubmittedBy(submittedBy);
        // In production: send to regulatory API and get submission ID
        entity.setSubmissionId("SUB-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        return reportRepository.save(entity);
    }

    @Transactional
    public ReportEntity updateSubmissionStatus(String reportId, String status, String submissionId) {
        ReportEntity entity = getReport(reportId);
        entity.setStatus(status);
        if (submissionId != null) entity.setSubmissionId(submissionId);
        return reportRepository.save(entity);
    }

    private String buildFatfXml(String reportType, String reportId,
                                 String customerId, String amount,
                                 String currency, String channel) {
        String date = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<FATFReport xmlns=\"urn:fatf:report:1.0\">\n" +
            "  <ReportHeader>\n" +
            "    <ReportType>" + reportType + "</ReportType>\n" +
            "    <ReportId>" + reportId + "</ReportId>\n" +
            "    <SubmissionDate>" + date + "</SubmissionDate>\n" +
            "  </ReportHeader>\n" +
            "  <ReportBody>\n" +
            "    <Subject>\n" +
            "      <CustomerId>" + customerId + "</CustomerId>\n" +
            "    </Subject>\n" +
            "    <Transaction>\n" +
            "      <Amount currency=\"" + currency + "\">" + amount + "</Amount>\n" +
            "      <Channel>" + channel + "</Channel>\n" +
            "    </Transaction>\n" +
            "  </ReportBody>\n" +
            "</FATFReport>";
    }
}
