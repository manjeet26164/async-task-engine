package com.engine.taskflow.controller;

import com.engine.taskflow.dto.SubmitJobRequest;
import com.engine.taskflow.dto.SubmitJobResponse;
import com.engine.taskflow.exception.DuplicateJobException;
import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.service.TaskService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobController {

    private final TaskService taskService;

    @PostMapping("/submit")
    public ResponseEntity<SubmitJobResponse> submitJob(
            @RequestHeader(value = "Idempotency-Key", required = true) String idempotencyKey,
            @RequestBody SubmitJobRequest request) {

        String jobId = taskService.submitJob(idempotencyKey, request.getTaskType(), request.getPayload());

        SubmitJobResponse response = SubmitJobResponse.builder()
                .jobId(jobId)
                .status("QUEUED")
                .message("Job accepted for asynchronous processing")
                .build();

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobRecord> getJobById(@PathVariable("id") String id) {
        return taskService.getJobById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler(DuplicateJobException.class)
    public ResponseEntity<Map<String, String>> handleDuplicateJobException(DuplicateJobException ex) {
        log.warn("Handling DuplicateJobException: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "Conflict",
                "message", ex.getMessage()
        ));
    }
}
