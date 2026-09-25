package com.engine.taskflow;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.worker.JobWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class JobWorkerTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private ListOperations<String, String> listOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private JobRepository jobRepository;

    @Mock
    private ExecutorService workerThreadPool;

    private JobWorker jobWorker;

    @BeforeEach
    void setUp() {
        jobWorker = new JobWorker(stringRedisTemplate, jobRepository, workerThreadPool);
    }

    @Test
    void shouldSuccessfullyProcessJobWhenPayloadIsValid() {
        String jobId = "job-success-1";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-1")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"fileUrl\":\"https://example.com/img.png\"}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));

        List<JobStatus> statusTransitions = new ArrayList<>();
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> {
            JobRecord record = invocation.getArgument(0);
            statusTransitions.add(record.getStatus());
            return record;
        });

        jobWorker.processJob(jobId);

        verify(jobRepository, times(2)).save(jobRecord);
        assertEquals(2, statusTransitions.size());
        assertEquals(JobStatus.RUNNING, statusTransitions.get(0));
        assertEquals(JobStatus.COMPLETED, statusTransitions.get(1));
        assertEquals(JobStatus.COMPLETED, jobRecord.getStatus());
        verify(stringRedisTemplate).delete("idemp:idemp-1");
    }

    @Test
    void shouldScheduleExponentialBackoffWhenExecutionFailsAndRetriesRemain() {
        String jobId = "job-fail-1";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-1")
                .taskType("SEND_EMAIL")
                .payload("{\"fail\":true,\"error\":\"fail_network\"}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(stringRedisTemplate.opsForZSet()).thenReturn(zSetOperations);

        List<JobStatus> statusTransitions = new ArrayList<>();
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> {
            JobRecord record = invocation.getArgument(0);
            statusTransitions.add(record.getStatus());
            return record;
        });

        jobWorker.processJob(jobId);

        verify(jobRepository, times(2)).save(jobRecord);
        assertEquals(2, statusTransitions.size());
        assertEquals(JobStatus.RUNNING, statusTransitions.get(0));
        assertEquals(JobStatus.SCHEDULED, statusTransitions.get(1));
        assertEquals(1, jobRecord.getRetryCount());

        verify(zSetOperations).add(eq(JobWorker.DELAYED_QUEUE_KEY), eq(jobId), anyDouble());
        verifyNoInteractions(listOperations);
        verify(stringRedisTemplate, never()).delete(anyString());
    }

    @Test
    void shouldRouteToDlqWhenMaxRetriesExceeded() {
        String jobId = "job-fail-max";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-max")
                .taskType("SEND_EMAIL")
                .payload("{\"fail\":true,\"error\":\"fail_permanent\"}")
                .status(JobStatus.QUEUED)
                .retryCount(2)
                .maxRetries(3)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        List<JobStatus> statusTransitions = new ArrayList<>();
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> {
            JobRecord record = invocation.getArgument(0);
            statusTransitions.add(record.getStatus());
            return record;
        });

        jobWorker.processJob(jobId);

        verify(jobRepository, times(2)).save(jobRecord);
        assertEquals(2, statusTransitions.size());
        assertEquals(JobStatus.RUNNING, statusTransitions.get(0));
        assertEquals(JobStatus.FAILED, statusTransitions.get(1));
        assertEquals(3, jobRecord.getRetryCount());

        verify(listOperations).leftPush(JobWorker.DLQ_KEY, jobId);
        verify(listOperations, never()).leftPush(eq(JobWorker.ACTIVE_QUEUE_KEY), anyString());
        verify(stringRedisTemplate).delete("idemp:idemp-fail-max");
    }

    @Test
    void shouldRecoverStuckJobAndRequeueWhenRetriesRemain() {
        JobRecord stuckJob = JobRecord.builder()
                .id("stuck-job-1")
                .taskType("PAYMENT")
                .status(JobStatus.RUNNING)
                .retryCount(0)
                .maxRetries(3)
                .leaseVersion(1)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(eq("stuck-job-1"), eq(1), eq(JobStatus.QUEUED), eq(2), eq(1), any(LocalDateTime.class)))
                .thenReturn(1);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        jobWorker.recoverStuckJobs();

        assertEquals(1, stuckJob.getRetryCount());
        assertEquals(JobStatus.QUEUED, stuckJob.getStatus());
        assertEquals(2, stuckJob.getLeaseVersion());
        verify(jobRepository).updateLeaseAndStatusIfVersionMatches(eq("stuck-job-1"), eq(1), eq(JobStatus.QUEUED), eq(2), eq(1), any(LocalDateTime.class));
        verify(listOperations).leftPush(JobWorker.ACTIVE_QUEUE_KEY, "stuck-job-1");
    }

    @Test
    void shouldRouteStuckJobToDlqWhenMaxRetriesExceeded() {
        JobRecord stuckJob = JobRecord.builder()
                .id("stuck-job-max")
                .taskType("PAYMENT")
                .status(JobStatus.RUNNING)
                .retryCount(2)
                .maxRetries(3)
                .leaseVersion(1)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(eq("stuck-job-max"), eq(1), eq(JobStatus.FAILED), eq(2), eq(3), any(LocalDateTime.class)))
                .thenReturn(1);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        jobWorker.recoverStuckJobs();

        assertEquals(3, stuckJob.getRetryCount());
        assertEquals(JobStatus.FAILED, stuckJob.getStatus());
        assertEquals(2, stuckJob.getLeaseVersion());
        verify(jobRepository).updateLeaseAndStatusIfVersionMatches(eq("stuck-job-max"), eq(1), eq(JobStatus.FAILED), eq(2), eq(3), any(LocalDateTime.class));
        verify(listOperations).leftPush(JobWorker.DLQ_KEY, "stuck-job-max");
    }

    @Test
    void shouldHandleMissingJobRecordGracefully() {
        String jobId = "non-existent-job";
        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        jobWorker.processJob(jobId);

        verify(jobRepository, never()).save(any(JobRecord.class));
        verifyNoInteractions(stringRedisTemplate);
    }

    @Test
    void shouldRetryFindByIdAndSucceedWhenRecordBecomesAvailableOnRetry() {
        String jobId = "job-retry-available";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-retry")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"fileUrl\":\"https://example.com/img.png\"}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .leaseVersion(0)
                .build();

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(jobRecord));

        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        verify(jobRepository, times(3)).findById(jobId);
        assertEquals(JobStatus.COMPLETED, jobRecord.getStatus());
        assertEquals(1, jobRecord.getLeaseVersion());
        verify(jobRepository, times(2)).save(jobRecord);
    }

    @Test
    void shouldRetryFindByIdUpToMaxAttemptsBeforeGivingUp() {
        String jobId = "non-existent-job-3x";
        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        jobWorker.processJob(jobId);

        verify(jobRepository, times(3)).findById(jobId);
        verify(jobRepository, never()).save(any(JobRecord.class));
        verifyNoInteractions(stringRedisTemplate);
    }

    @Test
    void shouldCompleteNormalProcessingAndUpdateLeaseCorrectly() {
        String jobId = "job-lease-normal";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-lease-normal")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"action\":\"resize\"}")
                .status(JobStatus.QUEUED)
                .leaseVersion(0)
                .workerId(null)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        assertEquals(jobWorker.getWorkerId(), jobRecord.getWorkerId());
        assertEquals(1, jobRecord.getLeaseVersion());
        assertEquals(JobStatus.COMPLETED, jobRecord.getStatus());
        verify(jobRepository, times(2)).save(jobRecord);
        verify(stringRedisTemplate).delete("idemp:idemp-lease-normal");
    }

    @Test
    void shouldIgnoreSupersededWorkerCompletionWhenLeaseVersionChanged() {
        String jobId = "job-lease-superseded";
        JobRecord initialJob = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-superseded")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"action\":\"resize\"}")
                .status(JobStatus.QUEUED)
                .leaseVersion(1)
                .workerId(null)
                .build();

        JobRecord supersededJobInDb = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-superseded")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"action\":\"resize\"}")
                .status(JobStatus.QUEUED)
                .leaseVersion(3)
                .workerId("other-worker")
                .build();

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(initialJob))
                .thenReturn(Optional.of(supersededJobInDb));

        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        verify(jobRepository, times(1)).save(initialJob);
        verify(jobRepository, never()).save(supersededJobInDb);
        assertEquals(JobStatus.QUEUED, supersededJobInDb.getStatus());
        assertEquals(3, supersededJobInDb.getLeaseVersion());
    }

    @Test
    void shouldIgnoreSupersededWorkerFailureWhenLeaseVersionChanged() {
        String jobId = "job-fail-superseded";
        JobRecord initialJob = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-superseded")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"action\":\"fail\"}")
                .status(JobStatus.QUEUED)
                .leaseVersion(1)
                .build();

        JobRecord supersededJobInDb = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-superseded")
                .taskType("IMAGE_PROCESSING")
                .payload("{\"action\":\"fail\"}")
                .status(JobStatus.QUEUED)
                .leaseVersion(3)
                .build();

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(initialJob))
                .thenReturn(Optional.of(supersededJobInDb));

        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        verify(jobRepository, times(1)).save(initialJob);
        verify(jobRepository, never()).save(supersededJobInDb);
        verify(stringRedisTemplate, never()).delete(anyString());
        verify(stringRedisTemplate, never()).opsForZSet();
    }

    @Test
    void shouldSkipWatchdogRecoveryWhenJobLeaseVersionWasChanged() {
        JobRecord stuckJob = JobRecord.builder()
                .id("stuck-superseded")
                .taskType("PAYMENT")
                .status(JobStatus.RUNNING)
                .retryCount(0)
                .leaseVersion(1)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(eq("stuck-superseded"), eq(1), any(), anyInt(), anyInt(), any()))
                .thenReturn(0);

        jobWorker.recoverStuckJobs();

        verify(jobRepository, never()).save(any(JobRecord.class));
        verify(listOperations, never()).leftPush(anyString(), anyString());
        verify(stringRedisTemplate, never()).delete(anyString());
    }

    @Test
    void shouldRetainIdempotencyKeyDuringRetriesAndReleaseOnlyOnTerminalState() {
        String jobId = "job-idemp-lifecycle";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("lifecycle-key-1")
                .taskType("DATA_SYNC")
                .payload("{\"fail\":true}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .leaseVersion(0)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(i -> i.getArgument(0));
        when(stringRedisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        jobWorker.processJob(jobId);
        assertEquals(JobStatus.SCHEDULED, jobRecord.getStatus());
        assertEquals(1, jobRecord.getRetryCount());
        verify(stringRedisTemplate, never()).delete("idemp:lifecycle-key-1");

        jobWorker.processJob(jobId);
        assertEquals(JobStatus.SCHEDULED, jobRecord.getStatus());
        assertEquals(2, jobRecord.getRetryCount());
        verify(stringRedisTemplate, never()).delete("idemp:lifecycle-key-1");

        jobWorker.processJob(jobId);
        assertEquals(JobStatus.FAILED, jobRecord.getStatus());
        assertEquals(3, jobRecord.getRetryCount());
        verify(stringRedisTemplate).delete("idemp:lifecycle-key-1");
    }

    @Test
    void shouldExecuteLuaScriptOnPollDelayedJobs() {
        when(stringRedisTemplate.execute(
                any(org.springframework.data.redis.core.script.RedisScript.class),
                any(List.class),
                any(String.class)
        )).thenReturn(3L);

        jobWorker.pollDelayedJobs();

        verify(stringRedisTemplate).execute(
                any(org.springframework.data.redis.core.script.RedisScript.class),
                eq(List.of(JobWorker.DELAYED_QUEUE_KEY, JobWorker.ACTIVE_QUEUE_KEY)),
                any(String.class)
        );
    }

    @Test
    void shouldNotThrowWhenPayloadHasFailFalse() {
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{\"fail\": false}"));
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{\"taskType\":\"PAYMENT\",\"fail\":false}"));
    }

    @Test
    void shouldThrowWhenPayloadHasFailTrue() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> jobWorker.simulatePayloadFailure("{\"fail\": true}"));
        assertEquals("Simulated network/system failure triggered by payload", ex.getMessage());

        RuntimeException ex2 = assertThrows(RuntimeException.class,
                () -> jobWorker.simulatePayloadFailure("{\"taskType\":\"PAYMENT\",\"fail\":true}"));
        assertEquals("Simulated network/system failure triggered by payload", ex2.getMessage());
    }

    @Test
    void shouldNotThrowWhenPayloadHasFailureReasonFieldWithoutFailField() {
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{\"failureReason\": \"something\"}"));
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{\"failureReason\": \"network timeout\", \"attempt\": 1}"));
    }

    @Test
    void shouldNotThrowWhenPayloadHasNoFailRelatedContent() {
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{\"userId\": \"user-123\", \"amount\": 250.0}"));
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{}"));
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure(null));
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure(""));
    }

    @Test
    void shouldNotThrowWhenPayloadIsMalformedJson() {
        assertDoesNotThrow(() -> jobWorker.simulatePayloadFailure("{malformed json text without closing brace"));
    }

    @Test
    void shouldCompleteSuccessfullyWhenPayloadHasFailFalseOrFailureReason() {
        String jobId = "job-no-false-positive-1";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-no-fp-1")
                .taskType("PAYMENT_GATEWAY")
                .payload("{\"failureReason\":\"previous_attempt_failed\",\"fail\":false}")
                .status(JobStatus.QUEUED)
                .retryCount(0)
                .maxRetries(3)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        assertEquals(JobStatus.COMPLETED, jobRecord.getStatus());
        verify(stringRedisTemplate).delete("idemp:idemp-no-fp-1");
    }

    @Test
    void shouldGracefullyHandleOptimisticLockingFailureWhenConcurrentSaveOccursInProcessJob() {
        String jobId = "job-concurrent-conflict";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-conflict-1")
                .taskType("IMAGE_PROCESSING")
                .payload("{}")
                .status(JobStatus.QUEUED)
                .leaseVersion(0)
                .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(jobRepository.save(any(JobRecord.class)))
                .thenReturn(jobRecord)
                .thenThrow(new ObjectOptimisticLockingFailureException(JobRecord.class, jobId));

        assertDoesNotThrow(() -> jobWorker.processJob(jobId));

        verify(stringRedisTemplate, never()).delete("idemp:idemp-conflict-1");
    }

    @Test
    void shouldGracefullyHandleOptimisticLockingFailureWhenWatchdogRecoversJobConcurrently() {
        String jobId = "job-watchdog-conflict";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-watchdog-conflict")
                .taskType("DATA_SYNC")
                .status(JobStatus.RUNNING)
                .leaseVersion(1)
                .retryCount(0)
                .maxRetries(3)
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(jobRecord));
        when(jobRepository.updateLeaseAndStatusIfVersionMatches(eq(jobId), eq(1), any(), anyInt(), anyInt(), any()))
                .thenReturn(0);

        assertDoesNotThrow(() -> jobWorker.recoverStuckJobs());

        verify(listOperations, never()).leftPush(anyString(), anyString());
    }

    @Test
    void shouldDispatchToRegisteredTaskHandlerWhenAvailable() throws Exception {
        String jobId = "job-custom-handler";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-custom-handler")
                .taskType("CUSTOM_DISPATCH")
                .payload("{\"test\": true}")
                .status(JobStatus.QUEUED)
                .leaseVersion(0)
                .build();

        com.engine.taskflow.handler.TaskHandler mockHandler = mock(com.engine.taskflow.handler.TaskHandler.class);
        when(mockHandler.getTaskType()).thenReturn("CUSTOM_DISPATCH");

        jobWorker.getTaskHandlerRegistry().register(mockHandler);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(jobRecord));
        when(jobRepository.save(any(JobRecord.class))).thenAnswer(invocation -> invocation.getArgument(0));

        jobWorker.processJob(jobId);

        verify(mockHandler).execute(jobRecord);
        assertEquals(JobStatus.COMPLETED, jobRecord.getStatus());
        verify(stringRedisTemplate).delete("idemp:idemp-custom-handler");
    }
}
