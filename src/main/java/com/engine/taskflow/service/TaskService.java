package com.engine.taskflow.service;

import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskService {

    public static final String IDEMPOTENCY_KEY_PREFIX = "idemp:";
    public static final String IDEMPOTENCY_LOCKED_VALUE = "LOCKED";
    public static final Duration IDEMPOTENCY_TTL = Duration.ofSeconds(60);
    public static final String ACTIVE_QUEUE_KEY = "jobs:queue:active";
    public static final String DELAYED_QUEUE_KEY = "jobs:queue:delayed";
    public static final String DLQ_KEY = "jobs:queue:dlq";

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
            .status(JobStatus.QUEUED)
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
            .status(JobStatus.SCHEDULED)
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
    public Optional<JobRecord> getJobById(String jobId) {
        return jobRepository.findById(jobId);
    }

    /**
     * Retrieves the top 20 most recent job records.
     *
     * @return list of recent JobRecord entities
     */
    @Transactional(readOnly = true)
    public List<JobRecord> getRecentJobs() {
        return jobRepository.findTop20ByOrderByCreatedAtDesc();
    }

    /**
     * Lists all jobs currently in Dead Letter Queue (DLQ).
     * Retrieves job IDs from the Redis DLQ list and populates the full JobRecord details from PostgreSQL.
     *
     * @return list of JobRecord entities residing in DLQ
     */
    @Transactional(readOnly = true)
    public List<JobRecord> getDlqJobs() {
        List<String> dlqJobIds = stringRedisTemplate.opsForList().range(DLQ_KEY, 0, -1);
        if (dlqJobIds == null || dlqJobIds.isEmpty()) {
            return jobRepository.findByStatus(JobStatus.FAILED);
        }

        Map<String, JobRecord> jobMap = jobRepository.findAllById(dlqJobIds).stream()
                .collect(Collectors.toMap(JobRecord::getId, Function.identity()));

        return dlqJobIds.stream()
                .map(jobMap::get)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Replays a dead-lettered job by removing it from the DLQ, resetting its status to QUEUED,
     * resetting retry count, and re-enqueueing it to the active processing queue.
     *
     * @param jobId ID of the dead-lettered job to replay
     * @return the updated JobRecord
     * @throws IllegalArgumentException if the job does not exist
     */
    @Transactional
    public JobRecord replayDlqJob(String jobId) {
        JobRecord job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found with ID: " + jobId));

        // Remove 1 occurrence from Redis DLQ list
        stringRedisTemplate.opsForList().remove(DLQ_KEY, 1, jobId);

        // Reset state for clean retry
        job.setStatus(JobStatus.QUEUED);
        job.setRetryCount(0);
        JobRecord saved = jobRepository.save(job);

        // Re-enqueue into active queue
        stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
        log.info("Job {} successfully replayed from DLQ to active queue", jobId);

        return saved;
    }
}
