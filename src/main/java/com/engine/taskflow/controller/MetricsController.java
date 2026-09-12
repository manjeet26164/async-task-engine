package com.engine.taskflow.controller;

import com.engine.taskflow.dto.MetricsResponse;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.worker.JobWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@RequestMapping("/api/v1/metrics")
@Slf4j
public class MetricsController {

    private final ThreadPoolExecutor threadPool;
    private final StringRedisTemplate stringRedisTemplate;
    private final JobRepository jobRepository;

    public MetricsController(
            @Qualifier("workerThreadPool") ThreadPoolExecutor threadPool,
            StringRedisTemplate stringRedisTemplate,
            JobRepository jobRepository) {
        this.threadPool = threadPool;
        this.stringRedisTemplate = stringRedisTemplate;
        this.jobRepository = jobRepository;
    }

    @GetMapping
    public ResponseEntity<MetricsResponse> getMetrics() {
        Long activeQueueSize = stringRedisTemplate.opsForList().size(JobWorker.ACTIVE_QUEUE_KEY);
        Long dlqQueueSize = stringRedisTemplate.opsForList().size(JobWorker.DLQ_KEY);
        Long delayedQueueSize = stringRedisTemplate.opsForZSet().zCard(JobWorker.DELAYED_QUEUE_KEY);

        MetricsResponse metrics = MetricsResponse.builder()
                .activeThreads(threadPool.getActiveCount())
                .poolSize(threadPool.getPoolSize())
                .corePoolSize(threadPool.getCorePoolSize())
                .queueRemainingCapacity(threadPool.getQueue().remainingCapacity())
                .queueCurrentSize(threadPool.getQueue().size())
                .activeRedisQueueLength(Optional.ofNullable(activeQueueSize).orElse(0L))
                .dlqRedisQueueLength(Optional.ofNullable(dlqQueueSize).orElse(0L))
                .delayedRedisQueueLength(Optional.ofNullable(delayedQueueSize).orElse(0L))
                .completedJobsCount(jobRepository.countByStatus(JobStatus.COMPLETED))
                .failedJobsCount(jobRepository.countByStatus(JobStatus.FAILED))
                .build();

        return ResponseEntity.ok(metrics);
    }
}
