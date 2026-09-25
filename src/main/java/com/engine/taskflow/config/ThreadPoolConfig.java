package com.engine.taskflow.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class ThreadPoolConfig {

    @Bean(name = "workerThreadPool", destroyMethod = "shutdown")
    public ThreadPoolExecutor workerThreadPool() {
        int corePoolSize = 4;
        int maxPoolSize = 8;
        long keepAliveTime = 60L;
        int queueCapacity = 100;

        ThreadFactory customThreadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "task-worker-" + threadNumber.getAndIncrement());
                thread.setDaemon(false);
                return thread;
            }
        };

        // CallerRunsPolicy provides backpressure: when the bounded queue fills up,
        // the submitting thread executes the task, naturally slowing down ingestion.
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                customThreadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
