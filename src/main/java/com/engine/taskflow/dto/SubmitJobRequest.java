package com.engine.taskflow.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
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
public class SubmitJobRequest {

    @NotBlank(message = "taskType is required and must not be blank")
    @Size(max = 100, message = "taskType must not exceed 100 characters")
    @Pattern(regexp = "^[A-Za-z0-9_-]+$", message = "taskType must contain only alphanumeric characters, underscores, and hyphens")
    private String taskType;

    @Size(max = 65536, message = "payload must not exceed 64KB (65536 characters)")
    private String payload;

    @Positive(message = "delayInSeconds must be a positive number")
    private Long delayInSeconds;
}
