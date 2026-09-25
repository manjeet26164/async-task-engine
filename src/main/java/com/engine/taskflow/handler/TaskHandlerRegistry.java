package com.engine.taskflow.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

// Thread-safe registry mapping task types to their handlers
@Component
@Slf4j
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlerMap = new ConcurrentHashMap<>();

    @Autowired(required = false)
    public TaskHandlerRegistry(List<TaskHandler> handlers) {
        if (handlers != null) {
            for (TaskHandler handler : handlers) {
                register(handler);
            }
        }
    }

    public TaskHandlerRegistry() {
    }

    public void register(TaskHandler handler) {
        if (handler != null && handler.getTaskType() != null && !handler.getTaskType().isBlank()) {
            String key = handler.getTaskType().trim().toUpperCase();
            handlerMap.put(key, handler);
            log.info("Registered TaskHandler [{}] for taskType [{}]",
                    handler.getClass().getSimpleName(), key);
        }
    }

    public Optional<TaskHandler> getHandler(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(handlerMap.get(taskType.trim().toUpperCase()));
    }

    public boolean hasHandler(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            return false;
        }
        return handlerMap.containsKey(taskType.trim().toUpperCase());
    }

    public int getRegisteredCount() {
        return handlerMap.size();
    }
}
