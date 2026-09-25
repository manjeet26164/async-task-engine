package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Task handler that evaluates payload-based failure simulation for testing and demos.
 */
@Component
@Slf4j
public class SimulationTaskHandler implements TaskHandler {

    private final ObjectMapper objectMapper;

    public SimulationTaskHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SimulationTaskHandler() {
        this(new ObjectMapper());
    }

    @Override
    public String getTaskType() {
        return "SIMULATION";
    }

    @Override
    public void execute(JobRecord job) throws Exception {
        log.info("Executing SimulationTaskHandler for jobId: {}", job.getId());
        String payload = job.getPayload();

        if (payload != null && !payload.isBlank()) {
            try {
                JsonNode rootNode = objectMapper.readTree(payload);
                if (rootNode != null && rootNode.has("fail") && rootNode.get("fail").asBoolean(false)) {
                    throw new RuntimeException("Simulated network/system failure triggered by payload");
                }
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                log.warn("Failed to parse JSON in SimulationTaskHandler for jobId: {}. Continuing.", job.getId());
            }
        }

        Thread.sleep(100);
        log.info("SimulationTaskHandler completed successfully for jobId: {}", job.getId());
    }
}
