package com.engine.taskflow;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.ANY)
@TestPropertySource(properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.flyway.enabled=true"
})
public class FlywaySchemaValidationTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TestEntityManager entityManager;

    @Test
    void shouldSuccessfullyApplyFlywayMigrationAndValidateHibernateSchema() {
        JobRecord job = JobRecord.builder()
                .id(UUID.randomUUID().toString())
                .idempotencyKey("idemp-flyway-test")
                .taskType("PAYMENT_PROCESSING")
                .payload("{\"amount\": 100}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .workerId("worker-test-1")
                .leaseVersion(1)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        JobRecord saved = jobRepository.save(job);
        assertNotNull(saved.getId());

        Optional<JobRecord> retrieved = jobRepository.findByIdempotencyKey("idemp-flyway-test");
        assertTrue(retrieved.isPresent());
        assertEquals("worker-test-1", retrieved.get().getWorkerId());
        assertEquals(1, retrieved.get().getLeaseVersion());
        assertEquals(0, retrieved.get().getOptimisticVersion());
        assertEquals(JobStatus.QUEUED, retrieved.get().getStatus());
    }

    @Test
    void shouldEnforceOptimisticLockingWhenTwoConcurrentSavesOccurWithSameStartingVersion() {
        JobRecord job = JobRecord.builder()
                .id(UUID.randomUUID().toString())
                .idempotencyKey("idemp-concurrent-opt-lock")
                .taskType("CONCURRENT_PROCESSING")
                .payload("{\"test\": true}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .workerId("worker-1")
                .leaseVersion(1)
                .build();

        jobRepository.saveAndFlush(job);

        // Fetch two distinct entity copies representing two concurrent transactions / threads
        JobRecord thread1Copy = entityManager.find(JobRecord.class, job.getId());
        entityManager.detach(thread1Copy);

        JobRecord thread2Copy = entityManager.find(JobRecord.class, job.getId());
        entityManager.detach(thread2Copy);

        assertEquals(0, thread1Copy.getOptimisticVersion());
        assertEquals(0, thread2Copy.getOptimisticVersion());

        // Thread 1 updates status and saves successfully (optimistic_version increments to 1)
        thread1Copy.setStatus(JobStatus.RUNNING);
        jobRepository.saveAndFlush(thread1Copy);

        // Thread 2 attempts to save with stale optimistic_version (0 instead of 1) -> must fail
        thread2Copy.setStatus(JobStatus.COMPLETED);
        assertThrows(ObjectOptimisticLockingFailureException.class, () -> {
            jobRepository.saveAndFlush(thread2Copy);
        });
    }
}
