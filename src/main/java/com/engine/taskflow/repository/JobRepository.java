package com.engine.taskflow.repository;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobRepository extends JpaRepository<JobRecord, String> {

    Optional<JobRecord> findByIdempotencyKey(String idempotencyKey);

    List<JobRecord> findByStatus(JobStatus status);

    Page<JobRecord> findByStatus(JobStatus status, Pageable pageable);

    long countByStatus(JobStatus status);

    List<JobRecord> findByStatusAndUpdatedAtBefore(JobStatus status, LocalDateTime threshold);

    List<JobRecord> findTop20ByOrderByCreatedAtDesc();

    Page<JobRecord> findAllByOrderByCreatedAtDesc(Pageable pageable);
}

