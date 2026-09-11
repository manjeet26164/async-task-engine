package com.engine.taskflow.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricsResponse {

    private int activeThreads;
    private int poolSize;
    private int corePoolSize;
    private int queueRemainingCapacity;
    private int queueCurrentSize;
    private long activeRedisQueueLength;
    private long dlqRedisQueueLength;
    private long delayedRedisQueueLength;
    private long completedJobsCount;
    private long failedJobsCount;
}
