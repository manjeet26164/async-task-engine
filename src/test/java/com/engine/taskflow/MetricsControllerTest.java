package com.engine.taskflow;

import com.engine.taskflow.controller.MetricsController;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.repository.JobRepository;
import com.engine.taskflow.worker.JobWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MetricsController.class)
@AutoConfigureMockMvc(addFilters = false)
@TestPropertySource(properties = {
    "app.security.api-key=test-secret-key-2026",
    "app.security.admin-api-key=test-admin-key-2026"
})
public class MetricsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean(name = "workerThreadPool")
    private ThreadPoolExecutor threadPool;

    @MockBean
    private StringRedisTemplate stringRedisTemplate;

    @MockBean
    private JobRepository jobRepository;

    @Test
    void shouldReturnSystemMetricsSuccessfully() throws Exception {
        ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(100);
        when(threadPool.getActiveCount()).thenReturn(2);
        when(threadPool.getPoolSize()).thenReturn(4);
        when(threadPool.getCorePoolSize()).thenReturn(4);
        when(threadPool.getQueue()).thenReturn(queue);

        ListOperations<String, String> listOps = mock(ListOperations.class);
        ZSetOperations<String, String> zsetOps = mock(ZSetOperations.class);
        when(stringRedisTemplate.opsForList()).thenReturn(listOps);
        when(stringRedisTemplate.opsForZSet()).thenReturn(zsetOps);
        when(listOps.size(JobWorker.ACTIVE_QUEUE_KEY)).thenReturn(5L);
        when(listOps.size(JobWorker.DLQ_KEY)).thenReturn(1L);
        when(zsetOps.zCard(JobWorker.DELAYED_QUEUE_KEY)).thenReturn(3L);

        when(jobRepository.countByStatus(JobStatus.COMPLETED)).thenReturn(50L);
        when(jobRepository.countByStatus(JobStatus.FAILED)).thenReturn(2L);

        mockMvc.perform(get("/api/v1/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.activeThreads").value(2))
                .andExpect(jsonPath("$.poolSize").value(4))
                .andExpect(jsonPath("$.corePoolSize").value(4))
                .andExpect(jsonPath("$.queueRemainingCapacity").value(100))
                .andExpect(jsonPath("$.activeRedisQueueLength").value(5))
                .andExpect(jsonPath("$.dlqRedisQueueLength").value(1))
                .andExpect(jsonPath("$.delayedRedisQueueLength").value(3))
                .andExpect(jsonPath("$.completedJobsCount").value(50))
                .andExpect(jsonPath("$.failedJobsCount").value(2));
    }
}
