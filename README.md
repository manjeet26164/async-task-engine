# ⚡ TaskFlow: Asynchronous Task Engine

An asynchronous background task processing engine built with Spring Boot 3.3.4, Java 17, PostgreSQL, and Redis. It demonstrates core systems engineering patterns including reliable queuing, idempotency, bounded concurrency, delayed scheduling, and watchdog crash recovery.

---

## Tech Stack

- **Java 17**
- **Spring Boot 3.3.4** (Web, Data JPA, Validation, Security)
- **PostgreSQL**
- **Redis**
- **Flyway**
- **Docker**
- **JUnit 5**

---

## Architecture Overview

```
                          ┌────────────────────────────────────────────────────────┐
                          │                   CLIENT INGESTION                     │
                          │   HTTP POST /api/v1/jobs/submit (with Idempotency-Key) │
                          └──────────────────────────┬─────────────────────────────┘
                                                     │
                                                     ▼
                                       ┌──────────────────────────────┐
                                       │       API Key Security       │
                                       │     & Jakarta Validation     │
                                       └──────────────┬───────────────┘
                                                     │
                                                     ▼
                                        ┌──────────────────────────────┐
                                        │   Redis Idempotency Guard    │
                                        │   SETNX idemp:<key> (24h)    │
                                        └──────────────┬───────────────┘
                                                     │
                          ┌──────────────────────────┴──────────────────────────┐
                          ▼                                                     ▼
           [ Immediate Task (delay = 0) ]                         [ Delayed Task (delay > 0) ]
                          │                                                     │
                          ▼                                                     ▼
        ┌───────────────────────────────────┐                 ┌───────────────────────────────────┐
        │  PostgreSQL: Status = QUEUED      │                 │  PostgreSQL: Status = SCHEDULED   │
        │  Redis: LPUSH jobs:queue:active   │                 │  Redis: ZADD jobs:queue:delayed   │
        └─────────────────┬─────────────────┘                 └─────────────────┬─────────────────┘
                          │                                                     │
                          │                                         ┌───────────▼───────────┐
                          │                                         │ @Scheduled (500ms)    │
                          │                                         │ Lua Script Promotion  │
                          │                                         └───────────┬───────────┘
                          │                                                     │
                          └──────────────────────────┬──────────────────────────┘
                                                     │
                                                     ▼
                                    ┌───────────────────────────────────┐
                                    │    JobWorker (BRPOPLPUSH into     │
                                    │  jobs:queue:processing:<worker>)  │
                                    └────────────────┬──────────────────┘
                                                     │
                                                     ▼
                                    ┌───────────────────────────────────┐
                                    │ Worker ThreadPool (Bounded Queue) │
                                    │   CallerRunsPolicy Backpressure   │
                                    └────────────────┬──────────────────┘
                                                     │
                                                     ▼
                                    ┌───────────────────────────────────┐
                                    │      TaskHandlerRegistry          │
                                    │  (FILE_EXPORT / WEBHOOK / SIM)    │
                                    └────────────────┬──────────────────┘
                                                     │
                          ┌──────────────────────────┴──────────────────────────┐
                          ▼                                                     ▼
                     [ SUCCESS ]                                           [ FAILURE ]
                          │                                                     │
                          ▼                                                     ▼
            ┌───────────────────────────┐                         ┌───────────────────────────┐
            │ PostgreSQL: COMPLETED     │                         │ Retry Count < MaxRetries? │
            │ Remove from processing q  │                         └─────────────┬─────────────┘
            └───────────────────────────┘                                       │
                                                   ┌────────────────────────────┴────────────────────────────┐
                                                   ▼                                                         ▼
                                                [ YES ]                                                   [ NO ]
                                                   │                                                         │
                                                   ▼                                                         ▼
                                 ┌───────────────────────────────────┐                     ┌───────────────────────────────────┐
                                 │ Exponential Backoff (2^retry sec) │                     │ PostgreSQL: Status = FAILED       │
                                 │ Redis: ZADD jobs:queue:delayed    │                     │ Redis: LPUSH jobs:queue:dlq       │
                                 │ PostgreSQL: Status = SCHEDULED    │                     │ (Dead Letter Queue)               │
                                 │ Remove from processing queue      │                     │ Remove from processing queue      │
                                 └───────────────────────────────────┘                     └─────────────────┬─────────────────┘
                                                                                                             │
                                                                                           ┌─────────────────▼─────────────────┐
                                                                                           │ Admin Replay (POST .../replay)    │
                                                                                           │ Resets retry & re-queues job      │
                                                                                           └───────────────────────────────────┘
```

---

## Core Patterns

- **Reliable Queuing**: Uses Redis `BRPOPLPUSH` into per-worker processing queues to prevent message loss on worker crash.
- **Request Idempotency**: Redis `SETNX` lock on `idemp:<key>` with 24-hour safety TTL; released when job reaches terminal state.
- **Pluggable Execution**: `TaskHandler` interface with handlers for disk file export, outbound HTTP webhooks, and failure simulation.
- **Atomic Watchdog Recovery**: Single-query compare-and-swap SQL update (`leaseVersion`) recovers orphaned jobs without race conditions.
- **Bounded Concurrency**: Custom `ThreadPoolExecutor` with bounded queue (100) and `CallerRunsPolicy` backpressure.
- **Delayed Scheduling**: Redis sorted set with epoch scores, promoted by an atomic Lua script every 500ms.
- **Exponential Backoff**: Failed jobs retry with exponential backoff delay ($2^{\text{retry}}$ seconds) before DLQ routing.
- **Dead Letter Queue (DLQ)**: Poison tasks route to DLQ with admin replay support.
- **Role-Based Security**: Stateless `X-API-KEY` header authentication with standard user and admin privilege tiers.

---

## Quick Start

### Prerequisites
- Java 17+
- Docker & Docker Compose (or Maven if running without Docker)

### 1. Run with Docker Compose
```bash
# Run complete stack (App + Postgres + Redis)
docker compose up --build -d

# Or run infrastructure only for local IDE development
docker compose up -d postgres redis
```

### 2. Run Locally with Maven
```bash
# Run all tests
./mvnw test

# Start application (requires local or containerized Postgres & Redis)
export APP_SECURITY_API_KEY="my-user-key"
export APP_SECURITY_ADMIN_API_KEY="my-admin-key"
./mvnw spring-boot:run
```

Dashboard is available at `http://localhost:8080/index.html`.

### Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `APP_SECURITY_API_KEY` | API key for standard user endpoints | None (required) |
| `APP_SECURITY_ADMIN_API_KEY` | API key for admin-only endpoints | None (required) |
| `SPRING_DATASOURCE_URL` | PostgreSQL JDBC connection URL | `jdbc:postgresql://localhost:5433/taskdb` |
| `SPRING_DATASOURCE_USERNAME` | PostgreSQL database username | `postgres` |
| `SPRING_DATASOURCE_PASSWORD` | PostgreSQL database password | `password` |
| `SPRING_REDIS_HOST` | Redis host | `localhost` |
| `SPRING_REDIS_PORT` | Redis port | `6379` |
| `APP_WORKER_STUCK_TIMEOUT_SECONDS` | Inactivity duration before stuck jobs are recovered | `300` |
| `APP_WORKER_RECOVERY_INTERVAL_MS` | Watchdog polling interval for stuck job recovery | `60000` |

---

## API Reference

All requests require the `X-API-KEY` header. Admin endpoints require the admin key.

| Method | Endpoint | Access | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/jobs/submit` | User / Admin | Submit immediate or delayed task |
| `GET` | `/api/v1/jobs/{id}` | User / Admin | Fetch job status and execution details |
| `GET` | `/api/v1/jobs/recent` | User / Admin | Paginated list of recent jobs |
| `GET` | `/api/v1/jobs/dlq` | Admin | Paginated list of DLQ failed jobs |
| `POST` | `/api/v1/jobs/dlq/{id}/replay` | Admin | Replay a failed job from DLQ |
| `GET` | `/api/v1/metrics` | User / Admin | Real-time thread pool and queue metrics |

### Example Request: Submit Job
```bash
curl -X POST http://localhost:8080/api/v1/jobs/submit \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: job-demo-101" \
  -H "X-API-KEY: my-user-key" \
  -d '{"taskType": "FILE_EXPORT", "payload": "{\"report\": \"summary\"}"}'
```

Response (`202 Accepted`):
```json
{
  "jobId": "c62a884d-2a62-43cf-bf2b-986c77840139",
  "status": "QUEUED",
  "message": "Job accepted for asynchronous processing"
}
```
*Note: All endpoints return standardized JSON errors (`ErrorResponse`) and paginated responses (`Page<JobRecord>`) following the same convention.*

---

## Known Limitations & Future Work

This engine is a portfolio and systems engineering learning project. Key limitations:

- **Single-Node Service**: No cluster leader election (e.g. ShedLock) for `@Scheduled` cron jobs across multiple app instances.
- **In-Process Handlers**: Task handlers execute within local JVM worker threads, not a distributed remote worker mesh.
- **Timestamp-Based Watchdog**: Detects orphaned jobs via `updatedAt` threshold rather than streaming heartbeats.
- **Future Improvements**: Distributed leader election, Prometheus metric export, and gRPC remote execution.
