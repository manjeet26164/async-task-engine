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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    }

    @Test
    void shouldScheduleExponentialBackoffWhenExecutionFailsAndRetriesRemain() {
        String jobId = "job-fail-1";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-1")
                .taskType("SEND_EMAIL")
                .payload("{\"error\":\"fail_network\"}")
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

        // Exponential backoff for retry 1: 2^1 = 2 seconds
        verify(zSetOperations).add(eq(JobWorker.DELAYED_QUEUE_KEY), eq(jobId), anyDouble());
        verifyNoInteractions(listOperations);
    }

    @Test
    void shouldRouteToDlqWhenMaxRetriesExceeded() {
        String jobId = "job-fail-max";
        JobRecord jobRecord = JobRecord.builder()
                .id(jobId)
                .idempotencyKey("idemp-fail-max")
                .taskType("SEND_EMAIL")
                .payload("{\"error\":\"fail_permanent\"}")
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
    }

    @Test
    void shouldRecoverStuckJobAndRequeueWhenRetriesRemain() {
        JobRecord stuckJob = JobRecord.builder()
                .id("stuck-job-1")
                .taskType("PAYMENT")
                .status(JobStatus.RUNNING)
                .retryCount(0)
                .maxRetries(3)
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        jobWorker.recoverStuckJobs();

        assertEquals(1, stuckJob.getRetryCount());
        assertEquals(JobStatus.QUEUED, stuckJob.getStatus());
        verify(jobRepository).save(stuckJob);
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
                .updatedAt(LocalDateTime.now().minusMinutes(10))
                .build();

        when(jobRepository.findByStatusAndUpdatedAtBefore(eq(JobStatus.RUNNING), any(LocalDateTime.class)))
                .thenReturn(List.of(stuckJob));
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);

        jobWorker.recoverStuckJobs();

        assertEquals(3, stuckJob.getRetryCount());
        assertEquals(JobStatus.FAILED, stuckJob.getStatus());
        verify(jobRepository).save(stuckJob);
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
}
