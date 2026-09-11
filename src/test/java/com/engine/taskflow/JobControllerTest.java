package com.engine.taskflow;

import com.engine.taskflow.controller.JobController;
import com.engine.taskflow.dto.SubmitJobRequest;
import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.service.TaskService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
public class JobControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @Autowired
        private ObjectMapper objectMapper;

        @MockBean
        private TaskService taskService;

        @Test
        void shouldReturnAcceptedWhenJobIsSubmittedSuccessfully() throws Exception {
                String idempotencyKey = "key-12345";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("IMAGE_PROCESSING")
                                .payload("{\"fileUrl\":\"https://example.com/image.png\"}")
                                .build();

                when(taskService.submitJob(eq(idempotencyKey), eq("IMAGE_PROCESSING"), eq(request.getPayload())))
                                .thenReturn("job-uuid-abc-123");

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isAccepted())
                                .andExpect(jsonPath("$.jobId").value("job-uuid-abc-123"))
                                .andExpect(jsonPath("$.status").value("QUEUED"));
        }

        @Test
        void shouldReturnConflictWhenDuplicateJobExceptionIsThrown() throws Exception {
                String idempotencyKey = "key-duplicate";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("SEND_EMAIL")
                                .payload("{\"email\":\"user@example.com\"}")
                                .build();

                when(taskService.submitJob(eq(idempotencyKey), eq("SEND_EMAIL"), eq(request.getPayload())))
                                .thenThrow(new DuplicateJobException(idempotencyKey,
                                                "A task with this idempotency key is already queued"));

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isConflict())
                                .andExpect(jsonPath("$.error").value("Conflict"))
                                .andExpect(jsonPath("$.message").isNotEmpty());
        }
}
