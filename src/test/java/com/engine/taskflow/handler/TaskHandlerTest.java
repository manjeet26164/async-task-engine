package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;
import com.engine.taskflow.model.JobStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class TaskHandlerTest {

    @Test
    void registryShouldRegisterAndRetrieveHandlersCaseInsensitively() {
        TaskHandler simulationHandler = new SimulationTaskHandler();
        TaskHandler fileHandler = new FileExportTaskHandler();
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(simulationHandler, fileHandler));

        assertEquals(2, registry.getRegisteredCount());
        assertTrue(registry.hasHandler("simulation"));
        assertTrue(registry.hasHandler("SIMULATION"));
        assertTrue(registry.hasHandler("file_export"));
        assertFalse(registry.hasHandler("UNKNOWN_TYPE"));

        Optional<TaskHandler> found = registry.getHandler("simulation");
        assertTrue(found.isPresent());
        assertEquals("SIMULATION", found.get().getTaskType());
    }

    @Test
    void simulationHandlerShouldExecuteNormallyWhenNoFailTriggered() {
        SimulationTaskHandler handler = new SimulationTaskHandler();
        JobRecord job = JobRecord.builder()
                .id("sim-job-1")
                .taskType("SIMULATION")
                .payload("{\"fail\": false, \"data\": 123}")
                .status(JobStatus.RUNNING)
                .build();

        assertDoesNotThrow(() -> handler.execute(job));
    }

    @Test
    void simulationHandlerShouldThrowExceptionWhenFailIsTrue() {
        SimulationTaskHandler handler = new SimulationTaskHandler();
        JobRecord job = JobRecord.builder()
                .id("sim-job-fail")
                .taskType("SIMULATION")
                .payload("{\"fail\": true}")
                .status(JobStatus.RUNNING)
                .build();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> handler.execute(job));
        assertTrue(ex.getMessage().contains("Simulated network/system failure"));
    }

    @Test
    void fileExportHandlerShouldWriteArtifactToDisk(@TempDir Path tempDir) throws Exception {
        FileExportTaskHandler handler = new FileExportTaskHandler(tempDir);
        String jobId = "export-job-999";
        JobRecord job = JobRecord.builder()
                .id(jobId)
                .taskType("FILE_EXPORT")
                .payload("{\"report\": \"monthly_summary\", \"rows\": 500}")
                .workerId("worker-test-export")
                .status(JobStatus.RUNNING)
                .build();

        handler.execute(job);

        Path expectedFile = tempDir.resolve("export-" + jobId + ".json");
        assertTrue(Files.exists(expectedFile), "Exported file should exist on disk");
        String content = Files.readString(expectedFile);
        assertTrue(content.contains(jobId));
        assertTrue(content.contains("monthly_summary"));
        assertTrue(content.contains("worker-test-export"));
    }

    @Test
    void httpWebhookHandlerShouldCompleteWhenNoUrlSpecified() {
        HttpWebhookTaskHandler handler = new HttpWebhookTaskHandler();
        JobRecord job = JobRecord.builder()
                .id("webhook-job-1")
                .taskType("WEBHOOK")
                .payload("{\"event\": \"payment_success\"}")
                .status(JobStatus.RUNNING)
                .build();

        assertDoesNotThrow(() -> handler.execute(job));
    }
}
