package com.engine.taskflow.service;

import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskService {

    private static final String IDEMPOTENCY_KEY_PREFIX = "idemp:";
    private static final String IDEMPOTENCY_LOCKED_VALUE = "LOCKED";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofSeconds(60);
    private static final String ACTIVE_QUEUE_KEY = "jobs:queue:active";

    private final StringRedisTemplate stringRedisTemplate;
    private final JobRepository jobRepository;

    /**
     * Submits a job by enforcing idempotency via Redis SETNX, saving the initial
     * record in PostgreSQL with status QUEUED, and pushing the jobId to the active Redis list.
     *
     * @param idempotencyKey unique idempotency token
     * @param taskType type of task
     * @param payload JSON string payload
     * @return the generated jobId
     * @throws DuplicateJobException if an identical idempotency key is locked or already processed
     */
    @Transactional
    public String submitJob(String idempotencyKey, String taskType, String payload) {
        String lockKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;

        // Step a: Enforce request idempotency using Redis SETNX with 60s TTL
        Boolean acquired = stringRedisTemplate.opsForValue().setIfAbsent(
            lockKey,
            IDEMPOTENCY_LOCKED_VALUE,
            IDEMPOTENCY_TTL
        );

        if (Boolean.FALSE.equals(acquired) || acquired == null) {
            log.warn("Duplicate submission rejected for idempotency key: {}", idempotencyKey);
            throw new DuplicateJobException(idempotencyKey, "A task with this idempotency key is already queued or in-flight");
        }

        // Step b: Persist initial record into PostgreSQL with status QUEUED
        JobRecord jobRecord = JobRecord.builder()
            .idempotencyKey(idempotencyKey)
            .taskType(taskType)
            .payload(payload)
            .status("QUEUED")
            .build();

        JobRecord savedRecord = jobRepository.save(jobRecord);
        String jobId = savedRecord.getId();

        // Step c: Push the jobId to Redis List "jobs:queue:active" using LPUSH
        stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
        log.info("Job successfully submitted and enqueued. jobId={}, taskType={}", jobId, taskType);

        // Step d: Return generated jobId
        return jobId;
    }
}
