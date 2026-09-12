package com.engine.taskflow.model;

/**
 * Represents the lifecycle state of an asynchronous job in TaskFlow.
 */
public enum JobStatus {
    QUEUED,
    SCHEDULED,
    RUNNING,
    COMPLETED,
    FAILED
}
