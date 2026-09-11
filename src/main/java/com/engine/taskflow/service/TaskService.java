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
    private static final String DELAYED_QUEUE_KEY = "jobs:queue:delayed";

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

    /**
     * Schedules a delayed job by enforcing idempotency, saving the initial record
     * in PostgreSQL with status SCHEDULED, and placing the jobId into the Redis Sorted Set.
     *
     * @param idempotencyKey unique idempotency token
     * @param taskType type of task
     * @param payload JSON string payload
     * @param delayInSeconds delay duration in seconds
     * @return the generated jobId
     * @throws DuplicateJobException if an identical idempotency key is already active
     */
    @Transactional
    public String scheduleDelayedJob(String idempotencyKey, String taskType, String payload, long delayInSeconds) {
        String lockKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;
        Duration lockTtl = Duration.ofSeconds(Math.max(60, delayInSeconds + 60));

        Boolean acquired = stringRedisTemplate.opsForValue().setIfAbsent(
            lockKey,
            IDEMPOTENCY_LOCKED_VALUE,
            lockTtl
        );

        if (Boolean.FALSE.equals(acquired) || acquired == null) {
            log.warn("Duplicate delayed submission rejected for idempotency key: {}", idempotencyKey);
            throw new DuplicateJobException(idempotencyKey, "A task with this idempotency key is already scheduled or in-flight");
        }

        JobRecord jobRecord = JobRecord.builder()
            .idempotencyKey(idempotencyKey)
            .taskType(taskType)
            .payload(payload)
            .status("SCHEDULED")
            .build();

        JobRecord savedRecord = jobRepository.save(jobRecord);
        String jobId = savedRecord.getId();

        double executeAt = (double) (System.currentTimeMillis() + (delayInSeconds * 1000L));
        stringRedisTemplate.opsForZSet().add(DELAYED_QUEUE_KEY, jobId, executeAt);
        log.info("Job scheduled with {}s delay. jobId={}, taskType={}, executeAt={}", delayInSeconds, jobId, taskType, executeAt);

        return jobId;
    }

    /**
     * Retrieves a JobRecord by its unique ID.
     *
     * @param jobId the ID of the job
     * @return an Optional containing the JobRecord if found
     */
    @Transactional(readOnly = true)
    public java.util.Optional<JobRecord> getJobById(String jobId) {
        return jobRepository.findById(jobId);
    }

    /**
     * Retrieves the top 20 most recent job records.
     *
     * @return list of recent JobRecord entities
     */
    @Transactional(readOnly = true)
    public java.util.List<JobRecord> getRecentJobs() {
        return jobRepository.findTop20ByOrderByCreatedAtDesc();
    }
}
