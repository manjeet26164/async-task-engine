package com.engine.taskflow.controller;

import com.engine.taskflow.dto.SubmitJobRequest;
import com.engine.taskflow.dto.SubmitJobResponse;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobController {

    private final TaskService taskService;

    @PostMapping("/submit")
    public ResponseEntity<SubmitJobResponse> submitJob(
            @RequestHeader(value = "Idempotency-Key", required = true) String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request) {

        String jobId;
        String status;
        String message;

        if (request.getDelayInSeconds() != null && request.getDelayInSeconds() > 0) {
            jobId = taskService.scheduleDelayedJob(
                    idempotencyKey,
                    request.getTaskType(),
                    request.getPayload(),
                    request.getDelayInSeconds()
            );
            status = "SCHEDULED";
            message = "Job scheduled with " + request.getDelayInSeconds() + "s delay";
        } else {
            jobId = taskService.submitJob(idempotencyKey, request.getTaskType(), request.getPayload());
            status = "QUEUED";
            message = "Job accepted for asynchronous processing";
        }

        SubmitJobResponse response = SubmitJobResponse.builder()
                .jobId(jobId)
                .status(status)
                .message(message)
                .build();

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/recent")
    public ResponseEntity<List<JobRecord>> getRecentJobs() {
        return ResponseEntity.ok(taskService.getRecentJobs());
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobRecord> getJobById(@PathVariable("id") String id) {
        return taskService.getJobById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/dlq")
    public ResponseEntity<List<JobRecord>> getDlqJobs() {
        return ResponseEntity.ok(taskService.getDlqJobs());
    }

    @PostMapping("/dlq/{id}/replay")
    public ResponseEntity<JobRecord> replayDlqJob(@PathVariable("id") String id) {
        JobRecord replayed = taskService.replayDlqJob(id);
        return ResponseEntity.ok(replayed);
    }
}
