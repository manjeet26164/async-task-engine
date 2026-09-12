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
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
    void shouldReturnRecentJobsList() {
        JobRecord r1 = JobRecord.builder().id("job-1").status(JobStatus.COMPLETED).build();
        when(jobRepository.findTop20ByOrderByCreatedAtDesc()).thenReturn(List.of(r1));

        List<JobRecord> recent = taskService.getRecentJobs();

        assertEquals(1, recent.size());
        assertEquals("job-1", recent.get(0).getId());
        verify(jobRepository).findTop20ByOrderByCreatedAtDesc();
    }

    @Test
    void shouldReturnDlqJobsFromRedisAndDatabase() {
        when(stringRedisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range(TaskService.DLQ_KEY, 0, -1)).thenReturn(List.of("dlq-job-1"));

        JobRecord dlqRecord = JobRecord.builder()
                .id("dlq-job-1")
                .taskType("PAYMENT")
                .status(JobStatus.FAILED)
                .retryCount(3)
                .build();

        when(jobRepository.findAllById(List.of("dlq-job-1"))).thenReturn(List.of(dlqRecord));

        List<JobRecord> dlqJobs = taskService.getDlqJobs();

        assertEquals(1, dlqJobs.size());
        assertEquals("dlq-job-1", dlqJobs.get(0).getId());
        assertEquals(JobStatus.FAILED, dlqJobs.get(0).getStatus());
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
}
