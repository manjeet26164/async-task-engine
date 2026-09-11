package com.engine.taskflow.dto;

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

    private String taskType;
    private String payload;
    private Long delayInSeconds;
}
