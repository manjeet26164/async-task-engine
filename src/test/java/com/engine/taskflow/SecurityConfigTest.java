package com.engine.taskflow;

import com.engine.taskflow.config.ApiKeyAuthFilter;
import com.engine.taskflow.config.SecurityConfig;
import com.engine.taskflow.controller.JobController;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.service.TaskService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
@Import({SecurityConfig.class, ApiKeyAuthFilter.class})
@TestPropertySource(properties = {
    "app.security.api-key=test-secret-key-2026",
    "app.security.admin-api-key=test-admin-key-2026"
})
public class SecurityConfigTest {

    private static final String VALID_API_KEY = "test-secret-key-2026";
    private static final String VALID_ADMIN_KEY = "test-admin-key-2026";
    private static final String INVALID_API_KEY = "invalid-key-xyz";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TaskService taskService;

    @Test
    void shouldAllowAccessWhenValidApiKeyIsProvided() throws Exception {
        when(taskService.getRecentJobs(any(Pageable.class))).thenReturn(Page.empty());

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

    @Test
    void shouldAllowAdminAccessToDlqWhenAdminApiKeyIsProvided() throws Exception {
        when(taskService.getDlqJobs(any(Pageable.class))).thenReturn(Page.empty());

        mockMvc.perform(get("/api/v1/jobs/dlq")
                        .header("X-API-KEY", VALID_ADMIN_KEY))
                .andExpect(status().isOk());
    }

    @Test
    void shouldForbidAccessToDlqWhenRegularApiKeyIsProvided() throws Exception {
        mockMvc.perform(get("/api/v1/jobs/dlq")
                        .header("X-API-KEY", VALID_API_KEY))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldAllowAdminAccessToDlqReplayWhenAdminApiKeyIsProvided() throws Exception {
        String jobId = "job-dlq-1";
        JobRecord replayed = JobRecord.builder()
                .id(jobId)
                .status(JobStatus.QUEUED)
                .build();
        when(taskService.replayDlqJob(jobId)).thenReturn(replayed);

        mockMvc.perform(post("/api/v1/jobs/dlq/" + jobId + "/replay")
                        .header("X-API-KEY", VALID_ADMIN_KEY))
                .andExpect(status().isOk());
    }

    @Test
    void shouldForbidAccessToDlqReplayWhenRegularApiKeyIsProvided() throws Exception {
        mockMvc.perform(post("/api/v1/jobs/dlq/job-dlq-1/replay")
                        .header("X-API-KEY", VALID_API_KEY))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldAllowAdminKeyAccessToRegularEndpoints() throws Exception {
        when(taskService.getRecentJobs(any(Pageable.class))).thenReturn(Page.empty());

        mockMvc.perform(get("/api/v1/jobs/recent")
                        .header("X-API-KEY", VALID_ADMIN_KEY))
                .andExpect(status().isOk());
    }

    @Test
    void shouldFailFastAtStartupWhenApiKeyIsBlank() {
        ApiKeyAuthFilter filter = new ApiKeyAuthFilter("   ", "admin-key", new ObjectMapper());
        IllegalStateException ex = assertThrows(IllegalStateException.class, filter::validateApiKeys);
        assertTrue(ex.getMessage().contains("app.security.api-key"));
    }

    @Test
    void shouldFailFastAtStartupWhenApiKeyIsNull() {
        ApiKeyAuthFilter filter = new ApiKeyAuthFilter(null, "admin-key", new ObjectMapper());
        IllegalStateException ex = assertThrows(IllegalStateException.class, filter::validateApiKeys);
        assertTrue(ex.getMessage().contains("app.security.api-key"));
    }

    @Test
    void shouldFailFastAtStartupWhenAdminApiKeyIsBlank() {
        ApiKeyAuthFilter filter = new ApiKeyAuthFilter("user-key", "   ", new ObjectMapper());
        IllegalStateException ex = assertThrows(IllegalStateException.class, filter::validateApiKeys);
        assertTrue(ex.getMessage().contains("app.security.admin-api-key"));
    }

    @Test
    void shouldFailFastAtStartupWhenAdminApiKeyIsNull() {
        ApiKeyAuthFilter filter = new ApiKeyAuthFilter("user-key", null, new ObjectMapper());
        IllegalStateException ex = assertThrows(IllegalStateException.class, filter::validateApiKeys);
        assertTrue(ex.getMessage().contains("app.security.admin-api-key"));
    }
}
