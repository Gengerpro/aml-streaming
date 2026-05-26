package com.aml.service.alert;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AlertRepository extends JpaRepository<AlertEntity, String> {
    List<AlertEntity> findByStatus(String status);
    List<AlertEntity> findByCustomerId(String customerId);
    List<AlertEntity> findBySeverityAndStatus(String severity, String status);

    @Query("SELECT a FROM AlertEntity a WHERE a.status = 'NEW' ORDER BY " +
           "CASE a.severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END, " +
           "a.createdAt ASC")
    List<AlertEntity> findNewAlertsOrderedByPriority();
}
