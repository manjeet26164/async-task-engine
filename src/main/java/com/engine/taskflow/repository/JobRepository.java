package com.engine.taskflow.repository;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobRepository extends JpaRepository<JobRecord, String> {

    Optional<JobRecord> findByIdempotencyKey(String idempotencyKey);

    List<JobRecord> findByStatus(JobStatus status);

    long countByStatus(JobStatus status);

    List<JobRecord> findByStatusAndUpdatedAtBefore(JobStatus status, LocalDateTime threshold);

    List<JobRecord> findTop20ByOrderByCreatedAtDesc();
}

