package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;

/**
 * Pluggable task handler interface for executing jobs dispatched by JobWorker.
 */
public interface TaskHandler {

    /**
     * Returns the task type string that this handler is responsible for (case-insensitive).
     */
    String getTaskType();

    /**
     * Executes the task. Throws an exception on failure to trigger retry/backoff.
     *
     * @param job the job record to process
     * @throws Exception if execution fails
     */
    void execute(JobRecord job) throws Exception;
}
