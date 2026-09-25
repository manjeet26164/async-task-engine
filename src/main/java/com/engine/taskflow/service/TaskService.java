package com.engine.taskflow.service;

import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

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
    public static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);
    public static final String ACTIVE_QUEUE_KEY = "jobs:queue:active";
    public static final String DELAYED_QUEUE_KEY = "jobs:queue:delayed";
    public static final String DLQ_KEY = "jobs:queue:dlq";

    private final StringRedisTemplate stringRedisTemplate;
    private final JobRepository jobRepository;

    @Transactional
    public String submitJob(String idempotencyKey, String taskType, String payload) {
        String lockKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;

        Boolean acquired = stringRedisTemplate.opsForValue().setIfAbsent(
            lockKey,
            IDEMPOTENCY_LOCKED_VALUE,
            IDEMPOTENCY_TTL
        );

        if (Boolean.FALSE.equals(acquired) || acquired == null) {
            log.warn("Duplicate submission rejected for idempotency key: {}", idempotencyKey);
            throw new DuplicateJobException(idempotencyKey, "A task with this idempotency key is already queued or in-flight");
        }

        JobRecord jobRecord = JobRecord.builder()
            .idempotencyKey(idempotencyKey)
            .taskType(taskType)
            .payload(payload)
            .status(JobStatus.QUEUED)
            .build();

        JobRecord savedRecord = jobRepository.save(jobRecord);
        String jobId = savedRecord.getId();

        // Enqueue in Redis only after the DB transaction commits so workers cannot read uncommitted state
        executeAfterTransactionCommit(() -> {
            stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
            log.info("Job successfully submitted and enqueued. jobId={}, taskType={}", jobId, taskType);
        });

        return jobId;
    }

    @Transactional
    public String scheduleDelayedJob(String idempotencyKey, String taskType, String payload, long delayInSeconds) {
        String lockKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;
        Duration lockTtl = Duration.ofSeconds(delayInSeconds + IDEMPOTENCY_TTL.toSeconds());

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
        // Add to Redis delayed set only after commit to avoid premature promotion
        executeAfterTransactionCommit(() -> {
            stringRedisTemplate.opsForZSet().add(DELAYED_QUEUE_KEY, jobId, executeAt);
            log.info("Job scheduled with {}s delay. jobId={}, taskType={}, executeAt={}", delayInSeconds, jobId, taskType, executeAt);
        });

        return jobId;
    }

    private void executeAfterTransactionCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }

    @Transactional(readOnly = true)
    public Optional<JobRecord> getJobById(String jobId) {
        return jobRepository.findById(jobId);
    }

    @Transactional(readOnly = true)
    public Page<JobRecord> getRecentJobs(Pageable pageable) {
        return jobRepository.findAllByOrderByCreatedAtDesc(pageable);
    }

    @Transactional(readOnly = true)
    public Page<JobRecord> getRecentJobs() {
        return getRecentJobs(PageRequest.of(0, 20));
    }

    // Reads job IDs from the Redis DLQ, falling back to PostgreSQL if Redis is empty.
    @Transactional(readOnly = true)
    public Page<JobRecord> getDlqJobs(Pageable pageable) {
        Long totalElements = stringRedisTemplate.opsForList().size(DLQ_KEY);
        if (totalElements == null || totalElements == 0) {
            return jobRepository.findByStatus(JobStatus.FAILED, pageable);
        }

        long start = pageable.getOffset();
        long end = start + pageable.getPageSize() - 1;
        List<String> dlqJobIds = stringRedisTemplate.opsForList().range(DLQ_KEY, start, end);

        if (dlqJobIds == null || dlqJobIds.isEmpty()) {
            return new PageImpl<>(Collections.emptyList(), pageable, totalElements);
        }

        Map<String, JobRecord> jobMap = jobRepository.findAllById(dlqJobIds).stream()
                .collect(Collectors.toMap(JobRecord::getId, Function.identity()));

        List<JobRecord> pageJobs = dlqJobIds.stream()
                .map(jobMap::get)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());

        return new PageImpl<>(pageJobs, pageable, totalElements);
    }

    @Transactional(readOnly = true)
    public Page<JobRecord> getDlqJobs() {
        return getDlqJobs(PageRequest.of(0, 20));
    }

    @Transactional
    public JobRecord replayDlqJob(String jobId) {
        JobRecord job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found with ID: " + jobId));

        stringRedisTemplate.opsForList().remove(DLQ_KEY, 1, jobId);

        job.setStatus(JobStatus.QUEUED);
        job.setRetryCount(0);
        JobRecord saved = jobRepository.save(job);

        // Re-enqueue in Redis only after the DB transaction commits
        executeAfterTransactionCommit(() -> {
            stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
            log.info("Job {} successfully replayed from DLQ to active queue", jobId);
        });

        return saved;
    }
}
