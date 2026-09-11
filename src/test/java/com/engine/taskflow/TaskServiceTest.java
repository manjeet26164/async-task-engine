package com.engine.taskflow;

import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
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

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                .status("QUEUED")
                .build();

        when(jobRepository.save(any(JobRecord.class))).thenReturn(savedRecord);

        String resultJobId = taskService.submitJob(idempotencyKey, taskType, payload);

        assertEquals("generated-job-id-999", resultJobId);
        verify(valueOperations).setIfAbsent(eq("idemp:" + idempotencyKey), eq("LOCKED"), any(Duration.class));
        verify(jobRepository).save(any(JobRecord.class));
        verify(listOperations).leftPush("jobs:queue:active", "generated-job-id-999");
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
        JobRecord record = JobRecord.builder().id(jobId).status("QUEUED").build();
        when(jobRepository.findById(jobId)).thenReturn(java.util.Optional.of(record));

        java.util.Optional<JobRecord> result = taskService.getJobById(jobId);

        assertEquals(true, result.isPresent());
        assertEquals("QUEUED", result.get().getStatus());
        verify(jobRepository).findById(jobId);
    }
}
