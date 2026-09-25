package com.engine.taskflow;

import com.engine.taskflow.handler.TaskHandlerRegistry;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.worker.JobWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class JobWorkerConcurrencyAndReliabilityTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private ListOperations<String, String> listOperations;

    @Mock
    private JobRepository jobRepository;

    @Mock
    private ExecutorService workerThreadPool;

    private JobWorker worker1;
    private JobWorker worker2;

    @BeforeEach
    void setUp() {
        worker1 = new JobWorker(stringRedisTemplate, jobRepository, workerThreadPool, new com.fasterxml.jackson.databind.ObjectMapper(), new TaskHandlerRegistry(), "worker-alpha");
        worker2 = new JobWorker(stringRedisTemplate, jobRepository, workerThreadPool, new com.fasterxml.jackson.databind.ObjectMapper(), new TaskHandlerRegistry(), "worker-beta");
    }

    @Test
    @DisplayName("Concurrency: Two workers attempting to claim the same job simultaneously - only one succeeds via optimistic locking")
    void twoWorkersClaimingSameJobSimultaneously_OnlyOneSucceeds() throws Exception {
        String jobId = "job-concurrent-race-1";
        JobRecord initialJob = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-race-1")
                .taskType("DATA_SYNC")
                .payload("{\"records\": 100}")
                .status(JobStatus.QUEUED)
                .leaseVersion(0)
                .optimisticVersion(0)
                .build();

        java.util.concurrent.atomic.AtomicReference<JobRecord> currentDbState = new java.util.concurrent.atomic.AtomicReference<>(
                JobRecord.builder()
                        .id(jobId)
                        .idempotencyKey("idemp-race-1")
                        .taskType("DATA_SYNC")
                        .payload("{\"records\": 100}")
                        .status(JobStatus.QUEUED)
                        .leaseVersion(0)
                        .optimisticVersion(0)
                        .build()
        );

        when(jobRepository.findById(jobId)).thenAnswer(inv -> {
            JobRecord r = currentDbState.get();
            return Optional.of(JobRecord.builder()
                    .id(r.getId())
                    .idempotencyKey(r.getIdempotencyKey())
                    .taskType(r.getTaskType())
                    .payload(r.getPayload())
                    .status(r.getStatus())
                    .workerId(r.getWorkerId())
                    .leaseVersion(r.getLeaseVersion())
                    .optimisticVersion(r.getOptimisticVersion())
                    .build());
        });

        AtomicInteger saveCalls = new AtomicInteger(0);
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> {
            saveCalls.incrementAndGet();
            JobRecord record = invocation.getArgument(0);
            // Simulate that worker-beta loses the race and collides with worker-alpha's update
            if ("worker-beta".equals(record.getWorkerId()) && record.getStatus() == JobStatus.RUNNING) {
                throw new ObjectOptimisticLockingFailureException(JobRecord.class, jobId);
            }
            // Update simulated database state on successful write
            currentDbState.set(JobRecord.builder()
                    .id(record.getId())
                    .idempotencyKey(record.getIdempotencyKey())
                    .taskType(record.getTaskType())
                    .payload(record.getPayload())
                    .status(record.getStatus())
                    .workerId(record.getWorkerId())
                    .leaseVersion(record.getLeaseVersion())
                    .optimisticVersion(record.getOptimisticVersion() + 1)
                    .build());
            return record;
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readyLatch = new CountDownLatch(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        Future<?> f1 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                worker1.processJob(jobId);
            } catch (Exception e) {
                fail(e);
            }
        });

        Future<?> f2 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                worker2.processJob(jobId);
            } catch (Exception e) {
                fail(e);
            }
        });

        assertTrue(readyLatch.await(3, TimeUnit.SECONDS));
        startLatch.countDown();

        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Worker-alpha saved RUNNING and COMPLETED (2 saves); worker-beta attempted 1 save that collided (1 save) -> total 3 attempts
        assertEquals(3, saveCalls.get());
        // Verify idempotency key was deleted only once by the winning worker
        verify(stringRedisTemplate, times(1)).delete("idemp:idemp-race-1");
    }

    @Test
    @DisplayName("Concurrency: Two watchdog instances attempt recovery of same stuck job - atomic CAS ensures only one succeeds")
    void twoWatchdogsRecoveringSameStuckJob_OnlyOneSucceedsViaAtomicCAS() throws Exception {
        String jobId = "stuck-job-cas-race";
        JobRecord stuckJob = JobRecord.builder()
                .id(jobId)
                .taskType("PAYMENT")
                .status(JobStatus.RUNNING)
                .retryCount(0)
                .maxRetries(3)
                .leaseVersion(1)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));

        // Atomic update query returns 1 for first watchdog, 0 for second watchdog
        AtomicInteger casCalls = new AtomicInteger(0);
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(
                eq(jobId), eq(1), eq(JobStatus.QUEUED), eq(2), eq(1), any(LocalDateTime.class)))
                .thenAnswer(invocation -> casCalls.incrementAndGet() == 1 ? 1 : 0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readyLatch = new CountDownLatch(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        Future<?> f1 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                worker1.recoverStuckJobs();
            } catch (Exception e) {
                fail(e);
            }
        });

        Future<?> f2 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                worker2.recoverStuckJobs();
            } catch (Exception e) {
                fail(e);
            }
        });

        assertTrue(readyLatch.await(3, TimeUnit.SECONDS));
        startLatch.countDown();

        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Exactly one watchdog successfully pushed the recovered job to the active queue
        verify(listOperations, times(1)).leftPush(JobWorker.ACTIVE_QUEUE_KEY, jobId);
    }

    @Test
    @DisplayName("Reliable Queue: Watchdog recovers orphaned job stuck in worker processing queue after simulated worker crash")
    void reliableQueue_WatchdogRecoversStuckJobFromProcessingQueueAfterWorkerCrash() {
        String crashedWorkerQueue = JobWorker.PROCESSING_QUEUE_PREFIX + "crashed-worker-777";
        String jobId = "job-crashed-worker-1";

        JobRecord orphanedJob = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-crashed-1")
                .taskType("FILE_EXPORT")
                .payload("{\"file\": \"report.pdf\"}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .leaseVersion(0)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(stringRedisTemplate.keys(JobWorker.PROCESSING_QUEUE_PREFIX + "*"))
                .thenReturn(Set.of(crashedWorkerQueue));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range(crashedWorkerQueue, 0, -1))
                .thenReturn(List.of(jobId));

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(orphanedJob));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(
                eq(jobId), eq(0), eq(JobStatus.QUEUED), eq(1), eq(1), any(LocalDateTime.class)))
                .thenReturn(1);

        worker1.recoverStuckProcessingJobs(LocalDateTime.now().minusSeconds(300));

        verify(listOperations).leftPush(JobWorker.ACTIVE_QUEUE_KEY, jobId);
        verify(listOperations).remove(crashedWorkerQueue, 1, jobId);
        assertEquals(JobStatus.QUEUED, orphanedJob.getStatus());
        assertEquals(1, orphanedJob.getRetryCount());
        assertEquals(1, orphanedJob.getLeaseVersion());
    }

    @Test
    @DisplayName("Reliable Queue: Watchdog routes stuck processing job to DLQ when max retries exceeded")
    void reliableQueue_WatchdogRoutesStuckProcessingJobToDlqWhenMaxRetriesExceeded() {
        String crashedWorkerQueue = JobWorker.PROCESSING_QUEUE_PREFIX + "crashed-worker-888";
        String jobId = "job-crashed-max-retries";

        JobRecord orphanedJob = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-crashed-max")
                .taskType("PAYMENT")
                .payload("{\"amount\": 99}")
                .status(JobStatus.QUEUED)
                .retryCount(2)
                .maxRetries(3)
                .leaseVersion(2)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(stringRedisTemplate.keys(JobWorker.PROCESSING_QUEUE_PREFIX + "*"))
                .thenReturn(Set.of(crashedWorkerQueue));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range(crashedWorkerQueue, 0, -1))
                .thenReturn(List.of(jobId));

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(orphanedJob));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(
                eq(jobId), eq(2), eq(JobStatus.FAILED), eq(3), eq(3), any(LocalDateTime.class)))
                .thenReturn(1);

        worker1.recoverStuckProcessingJobs(LocalDateTime.now().minusSeconds(300));

        verify(listOperations).leftPush(JobWorker.DLQ_KEY, jobId);
        verify(stringRedisTemplate).delete("idemp:idemp-crashed-max");
        verify(listOperations).remove(crashedWorkerQueue, 1, jobId);
        assertEquals(JobStatus.FAILED, orphanedJob.getStatus());
        assertEquals(3, orphanedJob.getRetryCount());
    }

    @Test
    @DisplayName("Reliable Queue: Zombie entry cleanup - removes already COMPLETED jobs from processing queue")
    void reliableQueue_CleansUpAlreadyCompletedJobFromProcessingQueue() {
        String crashedWorkerQueue = JobWorker.PROCESSING_QUEUE_PREFIX + "stale-worker-999";
        String jobId = "job-already-completed";

        JobRecord completedJob = JobRecord.builder()
                .id(jobId)
                .taskType("IMAGE_PROCESSING")
                .status(JobStatus.COMPLETED)
                .leaseVersion(1)
                .build();

        when(stringRedisTemplate.keys(JobWorker.PROCESSING_QUEUE_PREFIX + "*"))
                .thenReturn(Set.of(crashedWorkerQueue));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range(crashedWorkerQueue, 0, -1))
                .thenReturn(List.of(jobId));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(completedJob));

        worker1.recoverStuckProcessingJobs(LocalDateTime.now().minusSeconds(300));

        // Stale entry should be safely removed without re-queueing to active queue or DLQ
        verify(listOperations).remove(crashedWorkerQueue, 1, jobId);
        verify(listOperations, never()).leftPush(anyString(), anyString());
        verify(jobRepository, never()).updateLeaseAndStatusIfVersionMatches(any(), anyInt(), any(), anyInt(), anyInt(), any());
    }
}
