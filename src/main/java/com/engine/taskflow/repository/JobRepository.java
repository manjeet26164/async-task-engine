package com.engine.taskflow.repository;

import com.engine.taskflow.model.JobRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface JobRepository extends JpaRepository<JobRecord, String> {

    Optional<JobRecord> findByIdempotencyKey(String idempotencyKey);

    List<JobRecord> findByStatus(String status);

    long countByStatus(String status);

    List<JobRecord> findTop20ByOrderByCreatedAtDesc();
}
