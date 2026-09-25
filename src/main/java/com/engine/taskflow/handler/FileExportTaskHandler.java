package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

// Writes job execution records to JSON files on disk
@Component
@Slf4j
public class FileExportTaskHandler implements TaskHandler {

    private final Path exportDirectory;

    public FileExportTaskHandler() {
        this(Paths.get("target", "taskflow-exports"));
    }

    public FileExportTaskHandler(Path exportDirectory) {
        this.exportDirectory = exportDirectory;
    }

    @Override
    public String getTaskType() {
        return "FILE_EXPORT";
    }

    @Override
    public void execute(JobRecord job) throws Exception {
        log.info("Executing FileExportTaskHandler for jobId: {}", job.getId());

        if (!Files.exists(exportDirectory)) {
            Files.createDirectories(exportDirectory);
        }

        String safeJobId = job.getId().replaceAll("[^a-zA-Z0-9_-]", "_");
        Path targetFile = exportDirectory.resolve("export-" + safeJobId + ".json");

        String content = String.format(
                "{\n  \"jobId\": \"%s\",\n  \"taskType\": \"%s\",\n  \"workerId\": \"%s\",\n  \"executedAt\": \"%s\",\n  \"payload\": %s\n}",
                job.getId(),
                job.getTaskType(),
                job.getWorkerId() != null ? job.getWorkerId() : "unassigned",
                LocalDateTime.now(),
                (job.getPayload() != null && !job.getPayload().isBlank()) ? job.getPayload() : "{}"
        );

        Files.writeString(targetFile, content, StandardCharsets.UTF_8);
        long fileSize = Files.size(targetFile);

        log.info("FileExportTaskHandler successfully wrote {} bytes to file: {}", fileSize, targetFile.toAbsolutePath());
    }

    public Path getExportDirectory() {
        return exportDirectory;
    }
}
