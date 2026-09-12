package com.engine.taskflow.worker;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
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
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pollingThread;

    @Value("${app.worker.stuck-timeout-seconds:300}")
    private long stuckTimeoutSeconds = 300L;

    public JobWorker(
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository,
            @Qualifier("workerThreadPool") ExecutorService workerThreadPool) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.jobRepository = jobRepository;
        this.workerThreadPool = workerThreadPool;
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
        int updatedRetryCount = job.getRetryCount() + 1;
        job.setRetryCount(updatedRetryCount);
        int maxRetries = job.getMaxRetries() > 0 ? job.getMaxRetries() : 3;

        if (updatedRetryCount >= maxRetries) {
            job.setStatus(JobStatus.FAILED);
            jobRepository.save(job);
            stringRedisTemplate.opsForList().leftPush(DLQ_KEY, jobId);
            log.error("Stuck job {} exceeded max retries ({}/{}). Marked as FAILED and routed to DLQ",
                    jobId, updatedRetryCount, maxRetries);
        } else {
            job.setStatus(JobStatus.QUEUED);
            jobRepository.save(job);
            stringRedisTemplate.opsForList().leftPush(ACTIVE_QUEUE_KEY, jobId);
            log.warn("Stuck job {} recovered (retry {}/{}). Re-queued to active queue",
                    jobId, updatedRetryCount, maxRetries);
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

        Optional<JobRecord> optionalJob = jobRepository.findById(jobId);
        if (optionalJob.isEmpty()) {
            log.warn("[{}] Job record not found in PostgreSQL for jobId: {}", threadName, jobId);
            return;
        }

        JobRecord job = optionalJob.get();

        try {
            // Step 1: Update job status in PostgreSQL to RUNNING
            job.setStatus(JobStatus.RUNNING);
            jobRepository.save(job);
            log.info("[{}] Job {} marked as RUNNING in PostgreSQL", threadName, jobId);

            // Step 2: Simulate work with 500ms sleep
            Thread.sleep(500);

            // Step 3: Simulate failure if payload contains "fail"
            if (job.getPayload() != null && job.getPayload().contains("fail")) {
                throw new RuntimeException("Simulated network/system failure triggered by payload");
            }

            // Step 4: On success: set status to COMPLETED
            job.setStatus(JobStatus.COMPLETED);
            jobRepository.save(job);
            log.info("[{}] Job {} completed successfully. Marked as COMPLETED in PostgreSQL", threadName, jobId);

        } catch (Exception ex) {
            log.error("[{}] Error executing job {}: {}", threadName, jobId, ex.getMessage());
            handleJobFailure(job, ex);
        }
    }

    private void handleJobFailure(JobRecord job, Exception ex) {
        String threadName = Thread.currentThread().getName();
        String jobId = job.getId();
        int updatedRetryCount = job.getRetryCount() + 1;
        job.setRetryCount(updatedRetryCount);

        int maxRetries = job.getMaxRetries() > 0 ? job.getMaxRetries() : 3;

        if (updatedRetryCount >= maxRetries) {
            job.setStatus(JobStatus.FAILED);
            jobRepository.save(job);
            stringRedisTemplate.opsForList().leftPush(DLQ_KEY, jobId);
            log.error("[{}] Job {} reached max retries ({}/{}). Marked as FAILED and routed to DLQ [{}]",
                    threadName, jobId, updatedRetryCount, maxRetries, DLQ_KEY);
        } else {
            // Exponential backoff: 2^retryCount seconds (e.g. 2s, 4s, 8s...) capped at MAX_BACKOFF_SECONDS
            long backoffDelaySeconds = (long) Math.min(MAX_BACKOFF_SECONDS, Math.pow(2, updatedRetryCount));
            job.setStatus(JobStatus.SCHEDULED);
            jobRepository.save(job);

            double executeAt = (double) (System.currentTimeMillis() + (backoffDelaySeconds * 1000L));
            stringRedisTemplate.opsForZSet().add(DELAYED_QUEUE_KEY, jobId, executeAt);

            log.warn("[{}] Job {} failed (retry {}/{}). Exponential backoff delay {}s. Scheduled in Redis ZSET [{}]",
                    threadName, jobId, updatedRetryCount, maxRetries, backoffDelaySeconds, DELAYED_QUEUE_KEY);
        }
    }

    public void setStuckTimeoutSeconds(long stuckTimeoutSeconds) {
        this.stuckTimeoutSeconds = stuckTimeoutSeconds;
    }
}
