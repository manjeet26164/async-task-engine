package com.engine.taskflow.repository;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

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

    @Modifying(clearAutomatically = true)
    @Transactional
    @Query("UPDATE JobRecord j SET j.status = :newStatus, j.leaseVersion = :newLeaseVersion, " +
           "j.retryCount = :newRetryCount, j.workerId = null, j.updatedAt = :now, " +
           "j.optimisticVersion = j.optimisticVersion + 1 " +
           "WHERE j.id = :id AND j.leaseVersion = :expectedVersion")
    int updateLeaseAndStatusIfVersionMatches(
            @Param("id") String id,
            @Param("expectedVersion") int expectedVersion,
            @Param("newStatus") JobStatus newStatus,
            @Param("newLeaseVersion") int newLeaseVersion,
            @Param("newRetryCount") int newRetryCount,
            @Param("now") LocalDateTime now);
}

