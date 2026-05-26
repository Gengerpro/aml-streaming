package com.aml.service.case_mgt;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface CaseRepository extends JpaRepository<CaseEntity, String> {
    List<CaseEntity> findByStatus(String status);
    List<CaseEntity> findByCustomerId(String customerId);
    List<CaseEntity> findByAssignedTo(String assignedTo);

    @Query("SELECT c FROM CaseEntity c WHERE c.status IN ('OPEN', 'INVESTIGATING') ORDER BY " +
           "CASE c.priority WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END, " +
           "c.createdAt ASC")
    List<CaseEntity> findOpenCasesOrderedByPriority();

    long countByStatus(String status);
}
