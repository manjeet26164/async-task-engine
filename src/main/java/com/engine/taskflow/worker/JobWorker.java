package com.engine.taskflow.worker;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.service.TaskService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class JobWorker {

    public static final String ACTIVE_QUEUE_KEY = "jobs:queue:active";
    public static final String DLQ_KEY = "jobs:queue:dlq";
    public static final String DELAYED_QUEUE_KEY = "jobs:queue:delayed";
    private static final long POLL_TIMEOUT_SECONDS = 2L;
    private static final long MAX_BACKOFF_SECONDS = 300L;
    private static final int FIND_JOB_MAX_ATTEMPTS = 3;
    private static final long FIND_JOB_RETRY_DELAY_MS = 100L;

    private static final String MOVE_DELAYED_JOBS_LUA =
            "local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)\n" +
            "if #jobs > 0 then\n" +
            "    for _, jobId in ipairs(jobs) do\n" +
            "        redis.call('LPUSH', KEYS[2], jobId)\n" +
            "        redis.call('ZREM', KEYS[1], jobId)\n" +
            "    end\n" +
            "end\n" +
            "return #jobs";

    private final RedisScript<Long> moveDelayedScript = new DefaultRedisScript<>(MOVE_DELAYED_JOBS_LUA, Long.class);

    private final StringRedisTemplate stringRedisTemplate;
    private final JobRepository jobRepository;
    private final ExecutorService workerThreadPool;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final String workerId;
    private Thread pollingThread;

    @Value("${app.worker.stuck-timeout-seconds:300}")
    private long stuckTimeoutSeconds = 300L;

    @Autowired
    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            @Qualifier("workerThreadPool") ExecutorService workerThreadPool,
            ObjectMapper objectMapper) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, objectMapper, "worker-" + UUID.randomUUID());
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            ExecutorService workerThreadPool,
            ObjectMapper objectMapper,
            String workerId) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.jobRepository = jobRepository;
        this.workerThreadPool = workerThreadPool;
        this.objectMapper = objectMapper;
        this.workerId = workerId;
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            ExecutorService workerThreadPool) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, new ObjectMapper());
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            ExecutorService workerThreadPool,
            String workerId) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, new ObjectMapper(), workerId);
    }

    public String getWorkerId() {
        return workerId;
    }

    /**
     * Periodically polls mature delayed jobs from Redis Sorted Set and promotes them to the active queue.
     */
    @Scheduled(fixedRate = 500)
    public void pollDelayedJobs() {
        try {
            long now = System.currentTimeMillis();
            Long movedCount = stringRedisTemplate.execute(
                    moveDelayedScript,
                    List.of(DELAYED_QUEUE_KEY, ACTIVE_QUEUE_KEY),
                    String.valueOf(now)
            );
            if (movedCount != null && movedCount > 0) {
                log.info("Promoted {} mature delayed jobs from {} to {}", movedCount, DELAYED_QUEUE_KEY, ACTIVE_QUEUE_KEY);
            }
        } catch (Exception e) {
            log.error("Error promoting delayed jobs from Redis: {}", e.getMessage(), e);
        }
    }

    /**
     * Periodic watchdog task to detect and recover orphaned / stuck jobs.
     * If a worker crashed while executing a job (status remains RUNNING beyond the lease timeout),
     * this scheduled task re-enqueues the job or routes it to DLQ if retries are exhausted.
     */
    @Scheduled(fixedDelayString = "${app.worker.recovery-interval-ms:60000}")
    public void recoverStuckJobs() {
        try {
            LocalDateTime threshold = LocalDateTime.now().minusSeconds(stuckTimeoutSeconds);
            List<JobRecord> stuckJobs = jobRepository.findByStatusAndUpdatedAtBefore(JobStatus.RUNNING, threshold);

            if (!stuckJobs.isEmpty()) {
                log.warn("Watchdog detected {} stuck/orphaned job(s) in RUNNING state older than {}s",
                        stuckJobs.size(), stuckTimeoutSeconds);

                for (JobRecord job : stuckJobs) {
                    recoverOrphanedJob(job);
                }
            }
        } catch (Exception e) {
            log.error("Error during stuck job recovery watchdog scan: {}", e.getMessage(), e);
        }
    }

    private void recoverOrphanedJob(JobRecord job) {
        String jobId = job.getId();
        int expectedVersion = job.getLeaseVersion();

        Optional<JobRecord> currentJobOpt = jobRepository.findById(jobId);
        if (currentJobOpt.isEmpty() || currentJobOpt.get().getLeaseVersion() != expectedVersion) {
            log.warn("Watchdog recovery for job {} superseded. Expected leaseVersion {}, found {}. Skipping recovery.",
                    jobId, expectedVersion,
                    currentJobOpt.map(j -> String.valueOf(j.getLeaseVersion())).orElse("NOT_FOUND"));
            return;
        }

        JobRecord currentJob = currentJobOpt.get();
        int updatedRetryCount = currentJob.getRetryCount() + 1;
        currentJob.setRetryCount(updatedRetryCount);
        currentJob.setLeaseVersion(currentJob.getLeaseVersion() + 1);
        currentJob.setWorkerId(null);
        int maxRetries = currentJob.getMaxRetries() > 0 ? currentJob.getMaxRetries() : 3;

        if (updatedRetryCount >= maxRetries) {
            currentJob.setStatus(JobStatus.FAILED);
            try {
                jobRepository.save(currentJob);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("Watchdog recovery for job {} superseded by concurrent update (optimistic lock collision). Skipping recovery.", jobId);
                return;
            }
            stringRedisTemplate.opsForList().leftPush(DLQ_KEY, jobId);
            clearIdempotencyKey(currentJob);
            log.error("Stuck job {} exceeded max retries ({}/{}). Marked as FAILED and routed to DLQ",
                    jobId, updatedRetryCount, maxRetries);
        } else {
            currentJob.setStatus(JobStatus.QUEUED);
            try {
                jobRepository.save(currentJob);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("Watchdog recovery for job {} superseded by concurrent update (optimistic lock collision). Skipping recovery.", jobId);
                return;
            }
            stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
            log.warn("Stuck job {} recovered (retry {}/{}). Bumped leaseVersion to {}. Re-queued to active queue",
                    jobId, updatedRetryCount, maxRetries, currentJob.getLeaseVersion());
        }
    }

    @PostConstruct
    public void init() {
        running.set(true);
        pollingThread = new Thread(this::pollJobs, "job-poller-thread");
        pollingThread.setDaemon(true);
        pollingThread.start();
        log.info("JobWorker initialized and background polling thread started.");
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
        log.info("JobWorker shutdown signal received.");
    }

    private void pollJobs() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                String jobId = stringRedisTemplate.opsForList().rightPop(
                        ACTIVE_QUEUE_KEY,
                        POLL_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                );

                if (jobId != null && !jobId.isBlank()) {
                    log.info("[{}] Pulled jobId: {} from active queue, dispatching to workerThreadPool",
                            Thread.currentThread().getName(), jobId);
                    workerThreadPool.submit(() -> processJob(jobId));
                }
            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                log.error("[{}] Error polling from Redis active queue: {}",
                        Thread.currentThread().getName(), e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void processJob(String jobId) {
        String threadName = Thread.currentThread().getName();
        log.info("[{}] Starting processing for job: {}", threadName, jobId);

        Optional<JobRecord> optionalJob = Optional.empty();
        for (int attempt = 1; attempt <= FIND_JOB_MAX_ATTEMPTS; attempt++) {
            optionalJob = jobRepository.findById(jobId);
            if (optionalJob.isPresent()) {
                break;
            }
            if (attempt < FIND_JOB_MAX_ATTEMPTS) {
                log.warn("[{}] Job record not found in PostgreSQL on attempt {}/{} for jobId: {}. Retrying in {}ms...",
                        threadName, attempt, FIND_JOB_MAX_ATTEMPTS, jobId, FIND_JOB_RETRY_DELAY_MS);
                try {
                    Thread.sleep(FIND_JOB_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] Interrupted while waiting to retry findById for jobId: {}", threadName, jobId);
                    break;
                }
            }
        }

        if (optionalJob.isEmpty()) {
            log.warn("[{}] Job record not found in PostgreSQL after {} attempts for jobId: {}",
                    threadName, FIND_JOB_MAX_ATTEMPTS, jobId);
            return;
        }

        JobRecord job = optionalJob.get();
        int assignedLeaseVersion = job.getLeaseVersion() + 1;

        try {
            // Step 1: Update job status in PostgreSQL to RUNNING and claim lease
            job.setStatus(JobStatus.RUNNING);
            job.setWorkerId(this.workerId);
            job.setLeaseVersion(assignedLeaseVersion);
            try {
                jobRepository.save(job);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("[{}] Job {} lease claim superseded by concurrent update (optimistic lock collision). Skipping execution.", threadName, jobId);
                return;
            }
            log.info("[{}] Job {} marked as RUNNING by worker {} with leaseVersion {}",
                    threadName, jobId, this.workerId, assignedLeaseVersion);

            // Step 2: Simulate work with 500ms sleep
            Thread.sleep(500);

            // Step 3: Simulate failure if payload indicates "fail": true
            simulatePayloadFailure(job.getPayload());

            // Step 4: Before writing final COMPLETED status, re-fetch and verify leaseVersion
            Optional<JobRecord> currentJobOpt = jobRepository.findById(jobId);
            if (currentJobOpt.isEmpty() || currentJobOpt.get().getLeaseVersion() != assignedLeaseVersion) {
                log.warn("[{}] Job {} completion attempt superseded. Expected leaseVersion {}, found {}. Skipping DB update.",
                        threadName, jobId, assignedLeaseVersion,
                        currentJobOpt.map(j -> String.valueOf(j.getLeaseVersion())).orElse("NOT_FOUND"));
                return;
            }

            JobRecord currentJob = currentJobOpt.get();
            currentJob.setStatus(JobStatus.COMPLETED);
            try {
                jobRepository.save(currentJob);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("[{}] Job {} completion superseded by concurrent update (optimistic lock collision). Skipping DB update.", threadName, jobId);
                return;
            }
            log.info("[{}] Job {} completed successfully. Marked as COMPLETED in PostgreSQL", threadName, jobId);

            // Explicitly release idempotency key when job reaches terminal COMPLETED state
            clearIdempotencyKey(currentJob);

        } catch (Exception ex) {
            log.error("[{}] Error executing job {}: {}", threadName, jobId, ex.getMessage());
            handleJobFailure(jobId, assignedLeaseVersion, ex);
        }
    }

    private void handleJobFailure(String jobId, int assignedLeaseVersion, Exception ex) {
        String threadName = Thread.currentThread().getName();

        // Before writing final FAILED or SCHEDULED status, re-fetch and verify leaseVersion
        Optional<JobRecord> currentJobOpt = jobRepository.findById(jobId);
        if (currentJobOpt.isEmpty() || currentJobOpt.get().getLeaseVersion() != assignedLeaseVersion) {
            log.warn("[{}] Job {} failure handling superseded. Expected leaseVersion {}, found {}. Skipping DB update.",
                    threadName, jobId, assignedLeaseVersion,
                    currentJobOpt.map(j -> String.valueOf(j.getLeaseVersion())).orElse("NOT_FOUND"));
            return;
        }

        JobRecord currentJob = currentJobOpt.get();
        int updatedRetryCount = currentJob.getRetryCount() + 1;
        currentJob.setRetryCount(updatedRetryCount);

        int maxRetries = currentJob.getMaxRetries() > 0 ? currentJob.getMaxRetries() : 3;

        if (updatedRetryCount >= maxRetries) {
            currentJob.setStatus(JobStatus.FAILED);
            try {
                jobRepository.save(currentJob);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("[{}] Job {} failure handling superseded by concurrent update (optimistic lock collision). Skipping DB update.", threadName, jobId);
                return;
            }
            stringRedisTemplate.opsForList().leftPush(DLQ_KEY, jobId);
            clearIdempotencyKey(currentJob);
            log.error("[{}] Job {} reached max retries ({}/{}). Marked as FAILED and routed to DLQ [{}]",
                    threadName, jobId, updatedRetryCount, maxRetries, DLQ_KEY);
        } else {
            // Exponential backoff: 2^retryCount seconds (e.g. 2s, 4s, 8s...) capped at MAX_BACKOFF_SECONDS
            long backoffDelaySeconds = (long) Math.min(MAX_BACKOFF_SECONDS, Math.pow(2, updatedRetryCount));
            currentJob.setStatus(JobStatus.SCHEDULED);
            try {
                jobRepository.save(currentJob);
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("[{}] Job {} retry scheduling superseded by concurrent update (optimistic lock collision). Skipping DB update.", threadName, jobId);
                return;
            }

            double executeAt = (double) (System.currentTimeMillis() + (backoffDelaySeconds * 1000L));
            stringRedisTemplate.opsForZSet().add(DELAYED_QUEUE_KEY, jobId, executeAt);

            log.warn("[{}] Job {} failed (retry {}/{}). Exponential backoff delay {}s. Scheduled in Redis ZSET [{}]",
                    threadName, jobId, updatedRetryCount, maxRetries, backoffDelaySeconds, DELAYED_QUEUE_KEY);
        }
    }

    private void clearIdempotencyKey(JobRecord job) {
        if (job != null && job.getIdempotencyKey() != null && !job.getIdempotencyKey().isBlank()) {
            String idempKey = TaskService.IDEMPOTENCY_KEY_PREFIX + job.getIdempotencyKey();
            try {
                stringRedisTemplate.delete(idempKey);
                log.info("Released idempotency key [{}] for job {} in terminal state [{}]",
                        idempKey, job.getId(), job.getStatus());
            } catch (Exception e) {
                log.error("Failed to delete idempotency key [{}] for job {}: {}", idempKey, job.getId(), e.getMessage());
            }
        }
    }

    public void setStuckTimeoutSeconds(long stuckTimeoutSeconds) {
        this.stuckTimeoutSeconds = stuckTimeoutSeconds;
    }

    /**
     * Evaluates whether a failure should be simulated based on the payload JSON.
     * Parses the payload using Jackson ObjectMapper, checking the actual boolean value of the "fail" field.
     * Defaults to false if the field is absent, non-boolean, or if the payload is malformed/not valid JSON
     * (logging a warning instead of failing).
     *
     * @param payload the job payload string
     * @return true if payload is valid JSON and "fail" field evaluates to boolean true, false otherwise
     */
    public boolean shouldSimulateFailure(String payload) {
        if (payload == null || payload.isBlank()) {
            return false;
        }
        try {
            JsonNode rootNode = objectMapper.readTree(payload);
            if (rootNode != null && rootNode.has("fail")) {
                JsonNode failNode = rootNode.get("fail");
                return failNode != null && failNode.asBoolean(false);
            }
            return false;
        } catch (Exception e) {
            log.warn("Failed to parse job payload as JSON when evaluating failure condition: {}. Defaulting to fail=false.", e.getMessage());
            return false;
        }
    }

    /**
     * Throws a simulated RuntimeException if the payload contains "fail": true.
     *
     * @param payload the job payload string
     * @throws RuntimeException if simulated failure is triggered
     */
    public void simulatePayloadFailure(String payload) {
        if (shouldSimulateFailure(payload)) {
            throw new RuntimeException("Simulated network/system failure triggered by payload");
        }
    }
}
