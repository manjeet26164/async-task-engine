package com.engine.taskflow;

import com.engine.taskflow.controller.JobController;
import com.engine.taskflow.dto.SubmitJobRequest;
import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.exception.GlobalExceptionHandler;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import com.engine.taskflow.service.TaskService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
@Import(GlobalExceptionHandler.class)
@AutoConfigureMockMvc(addFilters = false)
@TestPropertySource(properties = {
                "app.security.api-key=test-secret-key-2026",
                "app.security.admin-api-key=test-admin-key-2026"
})
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
                                .andExpect(jsonPath("$.status").value("QUEUED"))
                                .andExpect(jsonPath("$.message").value("Job accepted for asynchronous processing"));
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

        @Test
        void shouldReturnBadRequestWhenIdempotencyKeyHeaderIsMissing() throws Exception {
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("IMAGE_PROCESSING")
                                .payload("{\"fileUrl\":\"https://example.com/image.png\"}")
                                .build();

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Bad Request"))
                                .andExpect(jsonPath("$.message").value(containsString("Idempotency-Key")));
        }

        @Test
        void shouldReturnBadRequestWhenTaskTypeIsBlank() throws Exception {
                String idempotencyKey = "key-invalid-blank";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("")
                                .payload("{\"test\":true}")
                                .build();

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Bad Request"))
                                .andExpect(jsonPath("$.errors.taskType").isNotEmpty());
        }

        @Test
        void shouldReturnBadRequestWhenTaskTypeContainsInvalidCharacters() throws Exception {
                String idempotencyKey = "key-invalid-tasktype-chars";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("<script>alert(1)</script>")
                                .payload("{\"test\":true}")
                                .build();

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Bad Request"))
                                .andExpect(jsonPath("$.errors.taskType").isNotEmpty());
        }

        @Test
        void shouldReturnBadRequestWhenDelayInSecondsIsNegative() throws Exception {
                String idempotencyKey = "key-invalid-delay";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("DATA_SYNC")
                                .payload("{\"test\":true}")
                                .delayInSeconds(-5L)
                                .build();

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Bad Request"))
                                .andExpect(jsonPath("$.errors.delayInSeconds").isNotEmpty());
        }

        @Test
        void shouldReturnJobRecordWhenJobExists() throws Exception {
                String jobId = "job-123";
                JobRecord record = JobRecord.builder()
                                .id(jobId)
                                .idempotencyKey("idemp-test")
                                .taskType("IMAGE_PROCESSING")
                                .payload("{\"data\":\"test\"}")
                                .status(JobStatus.COMPLETED)
                                .retryCount(0)
                                .maxRetries(3)
                                .build();

                when(taskService.getJobById(jobId)).thenReturn(Optional.of(record));

                mockMvc.perform(get("/api/v1/jobs/" + jobId))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.id").value(jobId))
                                .andExpect(jsonPath("$.status").value("COMPLETED"))
                                .andExpect(jsonPath("$.taskType").value("IMAGE_PROCESSING"));
        }

        @Test
        void shouldReturnNotFoundWhenJobDoesNotExist() throws Exception {
                String jobId = "unknown-job";
                when(taskService.getJobById(jobId)).thenReturn(Optional.empty());

                mockMvc.perform(get("/api/v1/jobs/" + jobId))
                                .andExpect(status().isNotFound());
        }

        @Test
        void shouldReturnAcceptedWhenDelayedJobIsSubmitted() throws Exception {
                String idempotencyKey = "key-delayed-1";
                SubmitJobRequest request = SubmitJobRequest.builder()
                                .taskType("DATA_SYNC")
                                .payload("{\"target\":\"db\"}")
                                .delayInSeconds(10L)
                                .build();

                when(taskService.scheduleDelayedJob(eq(idempotencyKey), eq("DATA_SYNC"), eq(request.getPayload()),
                                eq(10L)))
                                .thenReturn("job-delayed-xyz");

                mockMvc.perform(post("/api/v1/jobs/submit")
                                .header("Idempotency-Key", idempotencyKey)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andExpect(status().isAccepted())
                                .andExpect(jsonPath("$.jobId").value("job-delayed-xyz"))
                                .andExpect(jsonPath("$.status").value("SCHEDULED"))
                                .andExpect(jsonPath("$.message").value("Job scheduled with 10s delay"));
        }

        @Test
        void shouldReturnRecentJobsListWithDefaultPagination() throws Exception {
                JobRecord r1 = JobRecord.builder()
                                .id("job-r1")
                                .taskType("SEND_EMAIL")
                                .status(JobStatus.COMPLETED)
                                .build();

                Page<JobRecord> page = new PageImpl<>(List.of(r1), PageRequest.of(0, 20), 1);
                when(taskService.getRecentJobs(eq(PageRequest.of(0, 20)))).thenReturn(page);

                mockMvc.perform(get("/api/v1/jobs/recent"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.content[0].id").value("job-r1"))
                                .andExpect(jsonPath("$.content[0].status").value("COMPLETED"))
                                .andExpect(jsonPath("$.totalElements").value(1))
                                .andExpect(jsonPath("$.totalPages").value(1))
                                .andExpect(jsonPath("$.number").value(0))
                                .andExpect(jsonPath("$.size").value(20));
        }

        @Test
        void shouldReturnRecentJobsListWithCustomPagination() throws Exception {
                JobRecord r1 = JobRecord.builder()
                                .id("job-r2")
                                .taskType("PAYMENT")
                                .status(JobStatus.COMPLETED)
                                .build();

                Page<JobRecord> page = new PageImpl<>(List.of(r1), PageRequest.of(1, 10), 11);
                when(taskService.getRecentJobs(eq(PageRequest.of(1, 10)))).thenReturn(page);

                mockMvc.perform(get("/api/v1/jobs/recent?page=1&size=10"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.content[0].id").value("job-r2"))
                                .andExpect(jsonPath("$.totalElements").value(11))
                                .andExpect(jsonPath("$.totalPages").value(2))
                                .andExpect(jsonPath("$.number").value(1))
                                .andExpect(jsonPath("$.size").value(10));
        }

        @Test
        void shouldSanitizeNegativePaginationParameters() throws Exception {
                JobRecord r1 = JobRecord.builder()
                                .id("job-sanitized")
                                .taskType("DATA_SYNC")
                                .status(JobStatus.COMPLETED)
                                .build();

                Page<JobRecord> page = new PageImpl<>(List.of(r1), PageRequest.of(0, 20), 1);
                when(taskService.getRecentJobs(eq(PageRequest.of(0, 20)))).thenReturn(page);

                mockMvc.perform(get("/api/v1/jobs/recent?page=-1&size=0"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.content[0].id").value("job-sanitized"))
                                .andExpect(jsonPath("$.totalElements").value(1));
        }

        @Test
        void shouldReturnDlqJobsListWithDefaultPagination() throws Exception {
                JobRecord dlqJob = JobRecord.builder()
                                .id("dlq-101")
                                .taskType("PAYMENT")
                                .status(JobStatus.FAILED)
                                .retryCount(3)
                                .build();

                Page<JobRecord> page = new PageImpl<>(List.of(dlqJob), PageRequest.of(0, 20), 1);
                when(taskService.getDlqJobs(eq(PageRequest.of(0, 20)))).thenReturn(page);

                mockMvc.perform(get("/api/v1/jobs/dlq"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.content[0].id").value("dlq-101"))
                                .andExpect(jsonPath("$.content[0].status").value("FAILED"))
                                .andExpect(jsonPath("$.totalElements").value(1))
                                .andExpect(jsonPath("$.totalPages").value(1))
                                .andExpect(jsonPath("$.number").value(0))
                                .andExpect(jsonPath("$.size").value(20));
        }

        @Test
        void shouldReturnDlqJobsListWithCustomPagination() throws Exception {
                JobRecord dlqJob = JobRecord.builder()
                                .id("dlq-102")
                                .taskType("OCR")
                                .status(JobStatus.FAILED)
                                .retryCount(3)
                                .build();

                Page<JobRecord> page = new PageImpl<>(List.of(dlqJob), PageRequest.of(2, 5), 11);
                when(taskService.getDlqJobs(eq(PageRequest.of(2, 5)))).thenReturn(page);

                mockMvc.perform(get("/api/v1/jobs/dlq?page=2&size=5"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.content[0].id").value("dlq-102"))
                                .andExpect(jsonPath("$.totalElements").value(11))
                                .andExpect(jsonPath("$.totalPages").value(3))
                                .andExpect(jsonPath("$.number").value(2))
                                .andExpect(jsonPath("$.size").value(5));
        }

        @Test
        void shouldSuccessfullyReplayDlqJob() throws Exception {
                String jobId = "dlq-replayed-1";
                JobRecord replayed = JobRecord.builder()
                                .id(jobId)
                                .taskType("PAYMENT")
                                .status(JobStatus.QUEUED)
                                .retryCount(0)
                                .build();

                when(taskService.replayDlqJob(jobId)).thenReturn(replayed);

                mockMvc.perform(post("/api/v1/jobs/dlq/" + jobId + "/replay"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.id").value(jobId))
                                .andExpect(jsonPath("$.status").value("QUEUED"))
                                .andExpect(jsonPath("$.retryCount").value(0));
        }

        @Test
        void shouldReturnBadRequestWhenReplayingNonExistentJob() throws Exception {
                String jobId = "non-existent-id";
                when(taskService.replayDlqJob(jobId))
                                .thenThrow(new IllegalArgumentException("Job not found with ID: " + jobId));

                mockMvc.perform(post("/api/v1/jobs/dlq/" + jobId + "/replay"))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Bad Request"))
                                .andExpect(jsonPath("$.message").value("Job not found with ID: " + jobId));
        }
}
