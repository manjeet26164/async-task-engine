package com.engine.taskflow;

import com.engine.taskflow.config.ApiKeyAuthFilter;
import com.engine.taskflow.config.SecurityConfig;
import com.engine.taskflow.controller.JobController;
import com.engine.taskflow.service.TaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
@Import({SecurityConfig.class, ApiKeyAuthFilter.class})
public class SecurityConfigTest {

    private static final String VALID_API_KEY = "taskflow-secret-key-2026";
    private static final String INVALID_API_KEY = "invalid-key-xyz";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TaskService taskService;

    @Test
    void shouldAllowAccessWhenValidApiKeyIsProvided() throws Exception {
        when(taskService.getRecentJobs()).thenReturn(Collections.emptyList());

        mockMvc.perform(get("/api/v1/jobs/recent")
                        .header("X-API-KEY", VALID_API_KEY))
                .andExpect(status().isOk());
    }

    @Test
    void shouldReturnUnauthorizedWhenApiKeyIsMissing() throws Exception {
        mockMvc.perform(get("/api/v1/jobs/recent"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"))
                .andExpect(jsonPath("$.message").value("Invalid or missing API key in X-API-KEY header"));
    }

    @Test
    void shouldReturnUnauthorizedWhenApiKeyIsInvalid() throws Exception {
        mockMvc.perform(get("/api/v1/jobs/recent")
                        .header("X-API-KEY", INVALID_API_KEY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }
}
