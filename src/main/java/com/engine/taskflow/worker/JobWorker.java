package com.engine.taskflow.worker;

import com.engine.taskflow.handler.TaskHandler;
import com.engine.taskflow.handler.TaskHandlerRegistry;
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
import org.springframework.transaction.annotation.Transactional;

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
    public static final String PROCESSING_QUEUE_PREFIX = "jobs:queue:processing:";
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
    private final TaskHandlerRegistry taskHandlerRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final String workerId;
    private final String processingQueueKey;
    private Thread pollingThread;

    @Value("${app.worker.stuck-timeout-seconds:300}")
    private long stuckTimeoutSeconds = 300L;

    @Autowired
    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            @Qualifier("workerThreadPool") ExecutorService workerThreadPool,
            ObjectMapper objectMapper,
            TaskHandlerRegistry taskHandlerRegistry) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, objectMapper, taskHandlerRegistry, "worker-" + UUID.randomUUID());
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            ExecutorService workerThreadPool,
            ObjectMapper objectMapper,
            TaskHandlerRegistry taskHandlerRegistry,
            String workerId) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.jobRepository = jobRepository;
        this.workerThreadPool = workerThreadPool;
        this.objectMapper = objectMapper;
        this.taskHandlerRegistry = taskHandlerRegistry != null ? taskHandlerRegistry : new TaskHandlerRegistry();
        this.workerId = workerId;
        this.processingQueueKey = PROCESSING_QUEUE_PREFIX + workerId;
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            @Qualifier("workerThreadPool") ExecutorService workerThreadPool,
            ObjectMapper objectMapper) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, objectMapper, new TaskHandlerRegistry());
    }

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            ExecutorService workerThreadPool,
            ObjectMapper objectMapper,
            String workerId) {
        this(stringRedisTemplate, jobRepository, workerThreadPool, objectMapper, new TaskHandlerRegistry(), workerId);
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

    public TaskHandlerRegistry getTaskHandlerRegistry() {
        return taskHandlerRegistry;
    }

    public String getProcessingQueueKey() {
        return processingQueueKey;
    }

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

    // Watchdog to recover jobs stuck in RUNNING beyond the lease timeout, and jobs stuck in worker processing lists
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

            recoverStuckProcessingJobs(threshold);
        } catch (Exception e) {
            log.error("Error during stuck job recovery watchdog scan: {}", e.getMessage(), e);
        }
    }

    public void recoverStuckProcessingJobs(LocalDateTime threshold) {
        try {
            if (stringRedisTemplate == null) {
                return;
            }
            java.util.Set<String> processingKeys = stringRedisTemplate.keys(PROCESSING_QUEUE_PREFIX + "*");
            if (processingKeys == null || processingKeys.isEmpty()) {
                return;
            }

            for (String queueKey : processingKeys) {
                if (stringRedisTemplate.opsForList() == null) {
                    continue;
                }
                List<String> jobIds = stringRedisTemplate.opsForList().range(queueKey, 0, -1);
                if (jobIds == null || jobIds.isEmpty()) {
                    continue;
                }

                for (String jobId : jobIds) {
                    if (jobId == null || jobId.isBlank()) {
                        continue;
                    }
                    recoverSingleProcessingJob(queueKey, jobId, threshold);
                }
            }
        } catch (Exception e) {
            log.error("Error during processing queue recovery watchdog scan: {}", e.getMessage(), e);
        }
    }

    private void recoverSingleProcessingJob(String queueKey, String jobId, LocalDateTime threshold) {
        Optional<JobRecord> jobOpt = jobRepository.findById(jobId);
        if (jobOpt.isEmpty()) {
            log.warn("Job {} in processing queue {} not found in database. Removing from processing queue.", jobId, queueKey);
            stringRedisTemplate.opsForList().remove(queueKey, 1, jobId);
            return;
        }

        JobRecord job = jobOpt.get();
        if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED) {
            log.info("Job {} in processing queue {} is already in terminal state {}. Cleaning up from queue.",
                    jobId, queueKey, job.getStatus());
            stringRedisTemplate.opsForList().remove(queueKey, 1, jobId);
            return;
        }

        if (job.getStatus() == JobStatus.SCHEDULED) {
            log.info("Job {} in processing queue {} is already scheduled for retry. Cleaning up from queue.",
                    jobId, queueKey);
            stringRedisTemplate.opsForList().remove(queueKey, 1, jobId);
            return;
        }

        if (job.getStatus() == JobStatus.RUNNING) {
            return;
        }

        if (job.getStatus() == JobStatus.QUEUED) {
            LocalDateTime lastUpdate = job.getUpdatedAt() != null ? job.getUpdatedAt() : job.getCreatedAt();
            if (lastUpdate != null && lastUpdate.isBefore(threshold)) {
                log.warn("Watchdog detected stuck job {} in QUEUED state in processing queue {} older than threshold", jobId, queueKey);
                recoverOrphanedJob(job);
                stringRedisTemplate.opsForList().remove(queueKey, 1, jobId);
            }
        }
    }

    @Transactional
    public boolean recoverOrphanedJob(JobRecord job) {
        String jobId = job.getId();
        int expectedVersion = job.getLeaseVersion();
        int updatedRetryCount = job.getRetryCount() + 1;
        int maxRetries = job.getMaxRetries() > 0 ? job.getMaxRetries() : 3;
        int newLeaseVersion = expectedVersion + 1;
        LocalDateTime now = LocalDateTime.now();

        JobStatus targetStatus = updatedRetryCount >= maxRetries ? JobStatus.FAILED : JobStatus.QUEUED;

        int updatedRows = jobRepository.updateLeaseAndStatusIfVersionMatches(
                jobId,
                expectedVersion,
                targetStatus,
                newLeaseVersion,
                updatedRetryCount,
                now
        );

        if (updatedRows == 0) {
            log.warn("Watchdog recovery for job {} superseded. Expected leaseVersion {}, found different version or job missing. Skipping recovery.",
                    jobId, expectedVersion);
            return false;
        }

        job.setStatus(targetStatus);
        job.setRetryCount(updatedRetryCount);
        job.setLeaseVersion(newLeaseVersion);
        job.setWorkerId(null);
        job.setUpdatedAt(now);

        if (targetStatus == JobStatus.FAILED) {
            if (stringRedisTemplate != null && stringRedisTemplate.opsForList() != null) {
                stringRedisTemplate.opsForList().leftPush(DLQ_KEY, jobId);
            }
            clearIdempotencyKey(job);
            log.error("Stuck job {} exceeded max retries ({}/{}). Marked as FAILED and routed to DLQ",
                    jobId, updatedRetryCount, maxRetries);
        } else {
            if (stringRedisTemplate != null && stringRedisTemplate.opsForList() != null) {
                stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
            }
            log.warn("Stuck job {} recovered (retry {}/{}). Bumped leaseVersion to {}. Re-queued to active queue",
                    jobId, updatedRetryCount, maxRetries, newLeaseVersion);
        }

        return true;
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
                String jobId = stringRedisTemplate.opsForList().rightPopAndLeftPush(
                        ACTIVE_QUEUE_KEY,
                        processingQueueKey,
                        POLL_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                );

                if (jobId != null && !jobId.isBlank()) {
                    log.info("[{}] Pulled jobId: {} from active queue into {}, dispatching to workerThreadPool",
                            Thread.currentThread().getName(), jobId, processingQueueKey);
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

            executeTask(job);

            // Verify fencing token before marking completed
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

            clearIdempotencyKey(currentJob);

        } catch (Exception ex) {
            log.error("[{}] Error executing job {}: {}", threadName, jobId, ex.getMessage());
            handleJobFailure(jobId, assignedLeaseVersion, ex);
        } finally {
            removeFromProcessingQueue(jobId);
        }
    }

    private void executeTask(JobRecord job) throws Exception {
        Optional<TaskHandler> handlerOpt = taskHandlerRegistry.getHandler(job.getTaskType());
        if (handlerOpt.isPresent()) {
            log.info("[{}] Dispatching job {} (taskType: {}) to handler: {}",
                    Thread.currentThread().getName(), job.getId(), job.getTaskType(),
                    handlerOpt.get().getClass().getSimpleName());
            handlerOpt.get().execute(job);
        } else {
            log.debug("[{}] No dedicated handler for taskType: {}. Using default simulation.",
                    Thread.currentThread().getName(), job.getTaskType());
            Thread.sleep(100);
            simulatePayloadFailure(job.getPayload());
        }
    }

    private void removeFromProcessingQueue(String jobId) {
        try {
            if (stringRedisTemplate != null && stringRedisTemplate.opsForList() != null) {
                stringRedisTemplate.opsForList().remove(processingQueueKey, 1, jobId);
            }
        } catch (Exception e) {
            log.warn("[{}] Could not remove job {} from processing queue {}: {}",
                    Thread.currentThread().getName(), jobId, processingQueueKey, e.getMessage());
        }
    }

    private void handleJobFailure(String jobId, int assignedLeaseVersion, Exception ex) {
        String threadName = Thread.currentThread().getName();

        // Verify fencing token before updating status
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
            // Exponential backoff capped at MAX_BACKOFF_SECONDS
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

    public void simulatePayloadFailure(String payload) {
        if (shouldSimulateFailure(payload)) {
            throw new RuntimeException("Simulated network/system failure triggered by payload");
        }
    }
}
