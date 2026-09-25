package com.engine.taskflow.handler;

import com.engine.taskflow.model.JobRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Task handler that dispatches an HTTP request / Webhook.
 * Demonstrates real network I/O task dispatch.
 */
@Component
@Slf4j
public class HttpWebhookTaskHandler implements TaskHandler {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HttpWebhookTaskHandler(HttpClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public HttpWebhookTaskHandler(ObjectMapper objectMapper) {
        this(
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(3))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build(),
                objectMapper
        );
    }

    public HttpWebhookTaskHandler() {
        this(new ObjectMapper());
    }

    @Override
    public String getTaskType() {
        return "WEBHOOK";
    }

    @Override
    public void execute(JobRecord job) throws Exception {
        log.info("Executing HttpWebhookTaskHandler for jobId: {}", job.getId());
        String targetUrl = null;

        if (job.getPayload() != null && !job.getPayload().isBlank()) {
            try {
                JsonNode root = objectMapper.readTree(job.getPayload());
                if (root != null && root.has("url")) {
                    targetUrl = root.get("url").asText();
                }
            } catch (Exception e) {
                log.warn("Failed to parse JSON for webhook URL in jobId: {}. Message: {}", job.getId(), e.getMessage());
            }
        }

        if (targetUrl == null || targetUrl.isBlank()) {
            log.info("No specific 'url' in payload for jobId: {}. Simulating successful webhook dispatch.", job.getId());
            Thread.sleep(100);
            return;
        }

        log.info("Dispatching webhook HTTP POST to [{}] for jobId: {}", targetUrl, job.getId());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .header("Content-Type", "application/json")
                .header("User-Agent", "TaskFlow-Engine/1.0")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(
                        job.getPayload() != null ? job.getPayload() : "{}"
                ))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int statusCode = response.statusCode();
        log.info("Webhook HTTP call completed for jobId: {} with status code: {}", job.getId(), statusCode);

        if (statusCode >= 400) {
            throw new RuntimeException("Webhook endpoint responded with HTTP error status: " + statusCode);
        }
    }
}
