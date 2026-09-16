package com.engine.taskflow;

import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.service.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TaskServiceTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ListOperations<String, String> listOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private JobRepository jobRepository;

    @InjectMocks
    private TaskService taskService;

    @BeforeEach
    void setUp() {
        org.mockito.Mockito.lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void shouldSuccessfullySubmitJobWhenIdempotencyKeyIsNew() {
        String idempotencyKey = "new-key-1";
        String taskType = "SEND_EMAIL";
        String payload = "{\"to\":\"user@example.com\"}";

        when(valueOperations.setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class)))
                .thenReturn(true);

        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        JobRecord savedRecord = JobRecord.builder()
                .id("generated-job-id-999")
                .idempotencyKey(idempotencyKey)
                .taskType(taskType)
                .payload(payload)
                .status(JobStatus.QUEUED)
                .build();

        when(jobRepository.save(any(JobRecord.class))).thenReturn(savedRecord);

        String resultJobId = taskService.submitJob(idempotencyKey, taskType, payload);

        assertEquals("generated-job-id-999", resultJobId);
        verify(valueOperations).setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class));
        verify(jobRepository).save(any(JobRecord.class));
        verify(listOperations).leftPush(TaskService.ACTIVE_QUEUE_KEY, "generated-job-id-999");
    }

    @Test
    void shouldThrowDuplicateJobExceptionWhenKeyAlreadyExists() {
        String idempotencyKey = "duplicate-key-2";
        String taskType = "SEND_EMAIL";
        String payload = "{}";

        when(valueOperations.setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class)))
                .thenReturn(false);

        assertThrows(DuplicateJobException.class, () ->
                taskService.submitJob(idempotencyKey, taskType, payload)
        );

        verify(jobRepository, never()).save(any(JobRecord.class));
    }

    @Test
    void shouldFindJobByIdWhenJobExists() {
        String jobId = "job-lookup-1";
        JobRecord record = JobRecord.builder().id(jobId).status(JobStatus.QUEUED).build();
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(record));

        Optional<JobRecord> result = taskService.getJobById(jobId);

        assertTrue(result.isPresent());
        assertEquals(JobStatus.QUEUED, result.get().getStatus());
        verify(jobRepository).findById(jobId);
    }

    @Test
    void shouldSuccessfullyScheduleDelayedJob() {
        String idempotencyKey = "delayed-key-1";
        String taskType = "DATA_SYNC";
        String payload = "{\"batch\":100}";
        long delaySeconds = 15L;

        when(stringRedisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(valueOperations.setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class)))
                .thenReturn(true);

        JobRecord savedRecord = JobRecord.builder()
                .id("generated-delayed-id")
                .idempotencyKey(idempotencyKey)
                .taskType(taskType)
                .payload(payload)
                .status(JobStatus.SCHEDULED)
                .build();

        when(jobRepository.save(any(JobRecord.class))).thenReturn(savedRecord);

        String resultJobId = taskService.scheduleDelayedJob(idempotencyKey, taskType, payload, delaySeconds);

        assertEquals("generated-delayed-id", resultJobId);
        verify(jobRepository).save(any(JobRecord.class));
        verify(zSetOperations).add(eq(TaskService.DELAYED_QUEUE_KEY), eq("generated-delayed-id"), any(Double.class));
    }

    @Test
    void shouldReturnPaginatedRecentJobs() {
        JobRecord r1 = JobRecord.builder().id("job-1").status(JobStatus.COMPLETED).build();
        Pageable pageable = PageRequest.of(0, 20);
        Page<JobRecord> page = new PageImpl<>(List.of(r1), pageable, 1);
        when(jobRepository.findAllByOrderByCreatedAtDesc(pageable)).thenReturn(page);

        Page<JobRecord> recent = taskService.getRecentJobs(pageable);

        assertEquals(1, recent.getTotalElements());
        assertEquals("job-1", recent.getContent().get(0).getId());
        verify(jobRepository).findAllByOrderByCreatedAtDesc(pageable);
    }

    @Test
    void shouldReturnDlqJobsFromRedisAndDatabaseWithPagination() {
        Pageable pageable = PageRequest.of(0, 20);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.size(TaskService.DLQ_KEY)).thenReturn(1L);
        when(listOperations.range(TaskService.DLQ_KEY, 0L, 19L)).thenReturn(List.of("dlq-job-1"));

        JobRecord dlqRecord = JobRecord.builder()
                .id("dlq-job-1")
                .taskType("PAYMENT")
                .status(JobStatus.FAILED)
                .retryCount(3)
                .build();

        when(jobRepository.findAllById(List.of("dlq-job-1"))).thenReturn(List.of(dlqRecord));

        Page<JobRecord> dlqJobs = taskService.getDlqJobs(pageable);

        assertEquals(1, dlqJobs.getTotalElements());
        assertEquals("dlq-job-1", dlqJobs.getContent().get(0).getId());
        assertEquals(JobStatus.FAILED, dlqJobs.getContent().get(0).getStatus());
    }

    @Test
    void shouldFallbackToDatabaseWhenRedisDlqIsEmpty() {
        Pageable pageable = PageRequest.of(0, 20);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.size(TaskService.DLQ_KEY)).thenReturn(0L);

        JobRecord dbDlqJob = JobRecord.builder()
                .id("db-dlq-1")
                .taskType("EMAIL")
                .status(JobStatus.FAILED)
                .build();

        Page<JobRecord> dbPage = new PageImpl<>(List.of(dbDlqJob), pageable, 1);
        when(jobRepository.findByStatus(JobStatus.FAILED, pageable)).thenReturn(dbPage);

        Page<JobRecord> dlqJobs = taskService.getDlqJobs(pageable);

        assertEquals(1, dlqJobs.getTotalElements());
        assertEquals("db-dlq-1", dlqJobs.getContent().get(0).getId());
        verify(jobRepository).findByStatus(JobStatus.FAILED, pageable);
    }

    @Test
    void shouldSuccessfullyReplayDlqJob() {
        String jobId = "dlq-job-replay";
        JobRecord failedJob = JobRecord.builder()
                .id(jobId)
                .taskType("OCR")
                .status(JobStatus.FAILED)
                .retryCount(3)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(failedJob));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(i -> i.getArgument(0));

        JobRecord replayed = taskService.replayDlqJob(jobId);

        assertEquals(JobStatus.QUEUED, replayed.getStatus());
        assertEquals(0, replayed.getRetryCount());
        verify(listOperations).remove(TaskService.DLQ_KEY, 1, jobId);
        verify(listOperations).leftPush(TaskService.ACTIVE_QUEUE_KEY, jobId);
        verify(jobRepository).save(failedJob);
    }

    @Test
    void shouldEnsureJobPushedToRedisIsFindableByWorkerOnlyAfterTransactionCommit() {
        String idempotencyKey = "tx-sync-key";
        String taskType = "PAYMENT_PROCESS";
        String payload = "{\"amount\":100}";
        String generatedJobId = "job-tx-123";

        when(valueOperations.setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class)))
                .thenReturn(true);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        JobRecord savedRecord = JobRecord.builder()
                .id(generatedJobId)
                .idempotencyKey(idempotencyKey)
                .taskType(taskType)
                .payload(payload)
                .status(JobStatus.QUEUED)
                .build();

        when(jobRepository.save(any(JobRecord.class))).thenReturn(savedRecord);

        // Track whether the database transaction has committed
        AtomicBoolean transactionCommitted = new AtomicBoolean(false);

        // Before commit, database query returns empty (simulating uncommitted read / worker seeing nothing)
        // After commit, database query returns the record
        when(jobRepository.findById(generatedJobId)).thenAnswer(inv -> {
            if (transactionCommitted.get()) {
                return Optional.of(savedRecord);
            }
            return Optional.empty();
        });

        // Track what a worker sees at the exact moment the job is pushed to Redis
        AtomicBoolean findableWhenPushedToRedis = new AtomicBoolean(false);
        doAnswer(inv -> {
            String pushedJobId = inv.getArgument(1);
            Optional<JobRecord> found = jobRepository.findById(pushedJobId);
            findableWhenPushedToRedis.set(found.isPresent());
            return 1L;
        }).when(listOperations).leftPush(eq(TaskService.ACTIVE_QUEUE_KEY), eq(generatedJobId));

        // Start Spring transaction synchronization
        TransactionSynchronizationManager.initSynchronization();
        try {
            String returnedJobId = taskService.submitJob(idempotencyKey, taskType, payload);
            assertEquals(generatedJobId, returnedJobId);

            // Verify: Before transaction commits, job is NOT yet pushed to Redis
            verify(listOperations, never()).leftPush(anyString(), anyString());

            // Simulate database transaction commit
            transactionCommitted.set(true);
            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }

            // Verify: Redis push happened in afterCommit, and at that exact moment the job WAS findable in Postgres
            verify(listOperations).leftPush(TaskService.ACTIVE_QUEUE_KEY, generatedJobId);
            assertTrue(findableWhenPushedToRedis.get(), "Job pushed to Redis must be findable in Postgres at the moment it is pushed");
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void shouldEnsureDelayedJobPushedToRedisIsOnlyAddedAfterTransactionCommit() {
        String idempotencyKey = "delayed-tx-key";
        String taskType = "DATA_SYNC";
        String payload = "{}";
        String generatedJobId = "job-delayed-tx-123";

        when(valueOperations.setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class)))
                .thenReturn(true);
        when(stringRedisTemplate.opsForZSet()).thenReturn(zSetOperations);

        JobRecord savedRecord = JobRecord.builder()
                .id(generatedJobId)
                .idempotencyKey(idempotencyKey)
                .taskType(taskType)
                .payload(payload)
                .status(JobStatus.SCHEDULED)
                .build();

        when(jobRepository.save(any(JobRecord.class))).thenReturn(savedRecord);

        // Start Spring transaction synchronization
        TransactionSynchronizationManager.initSynchronization();
        try {
            String returnedJobId = taskService.scheduleDelayedJob(idempotencyKey, taskType, payload, 10L);
            assertEquals(generatedJobId, returnedJobId);

            // Verify: Before transaction commits, delayed job is NOT yet added to Redis ZSet
            verify(zSetOperations, never()).add(anyString(), anyString(), anyDouble());

            // Simulate transaction commit
            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }

            // Verify: Added only after commit
            verify(zSetOperations).add(eq(TaskService.DELAYED_QUEUE_KEY), eq(generatedJobId), anyDouble());
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
