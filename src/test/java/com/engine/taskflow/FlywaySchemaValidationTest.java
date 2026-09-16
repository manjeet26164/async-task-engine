package com.engine.taskflow;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        assertEquals(JobStatus.QUEUED, retrieved.get().getStatus());
    }
}
