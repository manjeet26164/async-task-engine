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

        /*
         * CallerRunsPolicy Backpressure Rationale:
         * ----------------------------------------
         * 1. Bounded Queue: An ArrayBlockingQueue (capacity 100) prevents unbounded
         * memory
         * allocation and protects the JVM from Out-Of-Memory (OOM) errors during
         * traffic spikes.
         * 2. Producer Throttling: When the queue is full and worker threads (up to
         * maxPoolSize)
         * are saturated, CallerRunsPolicy forces the submitting (caller) thread to
         * execute
         * the task synchronously instead of dropping it or queuing more tasks.
         * 3. Natural Flow Control: By engaging the caller thread in execution,
         * ingestion of
         * subsequent incoming tasks is naturally slowed down, allowing worker threads
         * to
         * drain the queue without memory exhaustion.
         */
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
