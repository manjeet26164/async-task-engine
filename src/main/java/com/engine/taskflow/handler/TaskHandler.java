package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;

public interface TaskHandler {
    String getTaskType();
    void execute(JobRecord job) throws Exception;
}
