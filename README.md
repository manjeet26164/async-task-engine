# ⚡ TaskFlow: Distributed Task Engine

> A high-throughput, resilient asynchronous task engine built with **Spring Boot 3.3.4**, **Java 17**, **PostgreSQL**, and **Redis**. Demonstrates mission-critical distributed systems patterns including **idempotent ingestion**, **bounded worker backpressure**, **delayed/scheduled task execution**, **exponential backoff retries**, **orphaned/stuck job recovery**, and **Dead Letter Queue (DLQ) replay**.

---

## 🏗️ Architecture Overview

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
                                    │    JobWorker Poller (BRPOPLPUSH)  │
                                    └────────────────┬──────────────────┘
                                                     │
                                                     ▼
                                    ┌───────────────────────────────────┐
                                    │ Worker ThreadPool (Bounded Queue) │
                                    │   CallerRunsPolicy Backpressure   │
                                    └────────────────┬──────────────────┘
                                                     │
                          ┌──────────────────────────┴──────────────────────────┐
                          ▼                                                     ▼
                     [ SUCCESS ]                                           [ FAILURE ]
                          │                                                     │
                          ▼                                                     ▼
            ┌───────────────────────────┐                         ┌───────────────────────────┐
            │ PostgreSQL: COMPLETED     │                         │ Retry Count < MaxRetries? │
            └───────────────────────────┘                         └─────────────┬─────────────┘
                                                                                │
                                                   ┌────────────────────────────┴────────────────────────────┐
                                                   ▼                                                         ▼
                                                [ YES ]                                                   [ NO ]
                                                   │                                                         │
                                                   ▼                                                         ▼
                                 ┌───────────────────────────────────┐                     ┌───────────────────────────────────┐
                                 │ Exponential Backoff (2^retry sec) │                     │ PostgreSQL: Status = FAILED       │
                                 │ Redis: ZADD jobs:queue:delayed    │                     │ Redis: LPUSH jobs:queue:dlq       │
                                 │ PostgreSQL: Status = SCHEDULED    │                     │ (Dead Letter Queue)               │
                                 └───────────────────────────────────┘                     └─────────────────┬─────────────────┘
                                                                                                             │
                                                                                           ┌─────────────────▼─────────────────┐
                                                                                           │ Admin Replay (POST .../replay)    │
                                                                                           │ Resets retry & re-queues job      │
                                                                                           └───────────────────────────────────┘
```

---

## 🌟 Core Distributed Systems Patterns

1. **Request Idempotency & Deduplication**:
   - Enforced via Redis `SETNX` on key `idemp:<idempotency_key>` with a 24-hour safety-net TTL.
   - The idempotency key is explicitly released/deleted only when the job reaches a terminal state (`COMPLETED` or `FAILED`/DLQ), ensuring jobs retrying with exponential backoff remain strictly deduplicated while in-flight.
   - Prevents duplicate job execution when client networks retry or drop connections.

2. **Bounded Concurrency & Backpressure**:
   - Backed by a custom `ThreadPoolExecutor` with a bounded `ArrayBlockingQueue(100)`.
   - Utilizes `ThreadPoolExecutor.CallerRunsPolicy` to slow down upstream producers when internal worker threads are saturated.

3. **Delayed & Scheduled Task Queuing**:
   - Future jobs are scheduled into a Redis Sorted Set (`jobs:queue:delayed`) using `epoch_timestamp_ms` as the score.
   - An atomic Lua script promotes mature delayed jobs into `jobs:queue:active` every 500ms without race conditions.

4. **Exponential Backoff Retries**:
   - When a job encounters transient execution errors, it is not retried immediately.
   - Delay increases exponentially ($2^{\text{retryCount}}$ seconds, capped at 300s) and is scheduled in the Redis delayed queue.

5. **Watchdog Orphaned/Stuck Job Recovery**:
   - Detects worker nodes that crashed or froze mid-execution (`JobStatus.RUNNING` older than lease threshold, default 300s).
   - Re-queues the orphaned job for execution or pushes it to DLQ if max retries are exhausted.

6. **Dead Letter Queue (DLQ) & Admin Replay**:
   - Poison-pill jobs exceeding max retries are routed to `jobs:queue:dlq` and marked as `FAILED`.
   - Provides admin endpoints to inspect DLQ payloads and replay failed tasks with a clean state.

7. **Role-Based API Security (RBAC)**:
   - Enforces Jakarta Bean Validation (`@NotBlank`, `@Size`, `@Positive`) on payload DTOs with centralized error sanitization.
   - Protected with a stateless `X-API-KEY` header filter using constant-time verification (`MessageDigest.isEqual`).
   - Supports two distinct privilege tiers:
     - **Regular User (`ROLE_API_USER`)**: Authorizes job submission, status lookups, and telemetry (`/submit`, `/recent`, `/{id}`, `/metrics`).
     - **Admin (`ROLE_ADMIN` & `ROLE_API_USER`)**: Required for privileged Dead Letter Queue management and replays (`/api/v1/jobs/dlq/**`).

---

## 🚀 Quick Start (Local Setup)

### 1. Prerequisites
- **Java 17+** (JDK 17 or JDK 21/22)
- **Docker & Docker Compose**

### 2. Start PostgreSQL & Redis Containers
Run the provided `docker-compose.yml` to spin up local database and cache instances:
```bash
docker-compose up -d
```
This starts:
- **PostgreSQL 15** on `localhost:5432` (database: `taskdb`, user: `postgres`, password: `password`)
- **Redis 7** on `localhost:6379` (with AOF persistence enabled)

### 3. Configure Environment Variables
You can configure the application using environment variables or a `.env` file. By default, sensible local fallbacks are configured in `application.properties`:

| Environment Variable | Description | Default Local Value |
| :--- | :--- | :--- |
| `SPRING_DATASOURCE_URL` | PostgreSQL JDBC Connection URL | `jdbc:postgresql://localhost:5432/taskdb` |
| `SPRING_DATASOURCE_USERNAME` | PostgreSQL Username | `postgres` |
| `SPRING_DATASOURCE_PASSWORD` | PostgreSQL Password | `password` |
| `SPRING_REDIS_HOST` | Redis Host | `localhost` |
| `SPRING_REDIS_PORT` | Redis Port | `6379` |
| `SPRING_REDIS_PASSWORD` | Redis Password | `(empty)` |
| `SPRING_REDIS_SSL` | Enable Redis SSL/TLS (for Upstash) | `false` |
| `APP_SECURITY_API_KEY` | API Key for regular client endpoints (`/submit`, `/recent`, `/{id}`, `/metrics`) | **REQUIRED (no default - fail-fast at startup)** |
| `APP_SECURITY_ADMIN_API_KEY` | Admin API Key for privileged DLQ endpoints (`/api/v1/jobs/dlq/**`) | **REQUIRED (no default - fail-fast at startup)** |
| `APP_WORKER_STUCK_TIMEOUT_SECONDS` | Lease timeout for RUNNING jobs | `300` |
| `APP_WORKER_RECOVERY_INTERVAL_MS` | Stuck job scan frequency | `60000` |

> [!IMPORTANT]
> Both `APP_SECURITY_API_KEY` and `APP_SECURITY_ADMIN_API_KEY` are **REQUIRED** with no default values. The engine validates these properties at startup and will fail fast with an error if either is missing or blank.

#### Example: Running with environment variables
```powershell
# In PowerShell:
$env:APP_SECURITY_API_KEY="<your-user-api-key>"
$env:APP_SECURITY_ADMIN_API_KEY="<your-admin-api-key>"
$env:SPRING_DATASOURCE_PASSWORD="mysecretpassword"
.\mvnw spring-boot:run
```
```bash
# In Linux / macOS:
export APP_SECURITY_API_KEY="<your-user-api-key>"
export APP_SECURITY_ADMIN_API_KEY="<your-admin-api-key>"
export SPRING_DATASOURCE_PASSWORD="mysecretpassword"
./mvnw spring-boot:run
```

### 4. Build and Run the Application
```bash
# Run tests
./mvnw test

# Start Spring Boot application
./mvnw spring-boot:run
```

### 5. Access the Web Telemetry Dashboard
Open your browser to:
👉 **[http://localhost:8080/index.html](http://localhost:8080/index.html)**

---

## 🗄️ Database Migrations (Flyway)

TaskFlow uses **Flyway** for version-controlled, production-safe schema migrations. Hibernate's schema generation is set to `spring.jpa.hibernate.ddl-auto=validate` to prevent accidental or unreviewed schema drift in production.

### Migration Files
All migration scripts reside in:
```text
src/main/resources/db/migration/
```

### Adding New Migrations Going Forward
1. **Naming Convention**:
   Create a new `.sql` file following Flyway's versioning pattern: `V<Version>__<Description>.sql` (note the **double underscore** `__`).
   - Example: `V2__add_priority_to_job_records.sql`
   - Example: `V3__create_audit_log_table.sql`

2. **Write Pure SQL**:
   Write standard PostgreSQL DDL statements:
   ```sql
   ALTER TABLE job_records ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0 NOT NULL;
   CREATE INDEX IF NOT EXISTS idx_job_records_priority ON job_records(priority);
   ```

3. **Synchronize JPA Entities**:
   Update corresponding entity classes (such as `JobRecord.java`) with matching `@Column` definitions. Since `ddl-auto=validate` is enabled, Hibernate verifies entity-to-table parity at startup and will fail fast if any mismatch is detected.

4. **Execution**:
   Migrations execute automatically on application startup prior to Hibernate initialization.

---

## 📡 API Reference

All `/api/v1/**` endpoints require the header:
`X-API-KEY: <your-configured-api-key>`

- **Standard Operations** (`/submit`, `/recent`, `/{id}`, `/metrics`): Accept either `APP_SECURITY_API_KEY` or `APP_SECURITY_ADMIN_API_KEY`.
- **Privileged DLQ Operations** (`/api/v1/jobs/dlq/**`): Strictly require `APP_SECURITY_ADMIN_API_KEY`.

### 1. Ingest / Submit a Job
- **Endpoint**: `POST /api/v1/jobs/submit`
- **Headers**:
  - `Content-Type: application/json`
  - `Idempotency-Key: <unique-uuid-or-string>` (Required)
  - `X-API-KEY: <your-configured-api-key>` (Required)
- **Request Body**:
```json
{
  "taskType": "IMAGE_PROCESSING",
  "payload": "{\"fileUrl\": \"https://example.com/asset.png\", \"action\": \"RESIZE\"}",
  "delayInSeconds": 0
}
```
- **Response (`202 Accepted`)**:
```json
{
  "jobId": "c62a884d-2a62-43cf-bf2b-986c77840139",
  "status": "QUEUED",
  "message": "Job accepted for asynchronous processing"
}
```

### 2. Schedule a Delayed Job
- **Endpoint**: `POST /api/v1/jobs/submit`
- **Request Body**:
```json
{
  "taskType": "DATA_SYNC",
  "payload": "{\"target\": \"warehouse\"}",
  "delayInSeconds": 15
}
```
- **Response (`202 Accepted`)**:
```json
{
  "jobId": "8bf73e1c-593b-417c-b1b3-04b321a08620",
  "status": "SCHEDULED",
  "message": "Job scheduled with 15s delay"
}
```

### 3. Get Job Status by ID
- **Endpoint**: `GET /api/v1/jobs/{id}`
- **Response (`200 OK`)**:
```json
{
  "id": "c62a884d-2a62-43cf-bf2b-986c77840139",
  "idempotencyKey": "order-101-payment",
  "taskType": "IMAGE_PROCESSING",
  "payload": "{\"fileUrl\": \"https://example.com/asset.png\"}",
  "status": "COMPLETED",
  "retryCount": 0,
  "maxRetries": 3,
  "createdAt": "2026-09-13T00:20:00",
  "updatedAt": "2026-09-13T00:20:01"
}
```

### 4. Fetch Recent Jobs (Paginated)
- **Endpoint**: `GET /api/v1/jobs/recent`
- **Query Parameters (Optional)**:
  - `page` (integer, default: `0`): Zero-based page index.
  - `size` (integer, default: `20`): Maximum page size.
- **Response (`200 OK`)**:
```json
{
  "content": [
    {
      "id": "c62a884d-2a62-43cf-bf2b-986c77840139",
      "idempotencyKey": "order-101-payment",
      "taskType": "IMAGE_PROCESSING",
      "payload": "{\"fileUrl\": \"https://example.com/asset.png\"}",
      "status": "COMPLETED",
      "retryCount": 0,
      "maxRetries": 3,
      "workerId": "worker-491a-b32c",
      "leaseVersion": 1,
      "createdAt": "2026-09-13T00:20:00",
      "updatedAt": "2026-09-13T00:20:01"
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20
  },
  "totalElements": 1,
  "totalPages": 1,
  "number": 0,
  "size": 20
}
```

### 5. Fetch Dead Letter Queue (DLQ) (Paginated)
- **Endpoint**: `GET /api/v1/jobs/dlq`
- **Authorization**: **Admin Only** (requires `X-API-KEY: <APP_SECURITY_ADMIN_API_KEY>`)
- **Query Parameters (Optional)**:
  - `page` (integer, default: `0`): Zero-based page index.
  - `size` (integer, default: `20`): Maximum page size.
- **Response (`200 OK`)**:
```json
{
  "content": [
    {
      "id": "f8173abc-9421-4d1a-821b-6b27814917a1",
      "idempotencyKey": "load-test-failed-12",
      "taskType": "PAYMENT_GATEWAY",
      "payload": "{\"fail\": true}",
      "status": "FAILED",
      "retryCount": 3,
      "maxRetries": 3,
      "workerId": "worker-491a-b32c",
      "leaseVersion": 3,
      "createdAt": "2026-09-13T00:20:00",
      "updatedAt": "2026-09-13T00:25:00"
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20
  },
  "totalElements": 1,
  "totalPages": 1,
  "number": 0,
  "size": 20
}
```

### 6. Replay a DLQ Job
- **Endpoint**: `POST /api/v1/jobs/dlq/{id}/replay`
- **Authorization**: **Admin Only** (requires `X-API-KEY: <APP_SECURITY_ADMIN_API_KEY>`)
- **Response (`200 OK`)**:
```json
{
  "id": "f8173abc-9421-4d1a-821b-6b27814917a1",
  "status": "QUEUED",
  "retryCount": 0
}
```

### 7. Real-Time Telemetry & Engine Metrics
- **Endpoint**: `GET /api/v1/metrics`
- **Response (`200 OK`)**:
```json
{
  "activeThreads": 4,
  "poolSize": 8,
  "corePoolSize": 4,
  "queueRemainingCapacity": 92,
  "queueCurrentSize": 8,
  "activeRedisQueueLength": 12,
  "dlqRedisQueueLength": 1,
  "delayedRedisQueueLength": 5,
  "completedJobsCount": 184,
  "failedJobsCount": 3
}
```

---

## 🧪 Concurrency & Backpressure Load Simulation

Run the included load simulator to fire 200 concurrent tasks (with a simulated failure rate) to observe real-time backpressure and queue consumption:

```powershell
# PowerShell (pass your configured API key)
.\simulate_load.ps1 -ApiKey "<your-configured-api-key>" -TotalRequests 200 -Concurrency 50
```

```bash
# Bash (pass your configured API key)
./simulate_load.sh http://localhost:8080/api/v1/jobs/submit <your-configured-api-key>
```

---

## 📌 Portfolio & Architecture Note

> **Note**: This project is built as a portfolio and educational system showcasing how to architect resilient distributed background workers in Java/Spring Boot without relying solely on heavyweight external orchestrators. It demonstrates concrete implementations of idempotency keys, Redis data structures (Lists, Sorted Sets), Lua scripts for atomic queue manipulation, thread pool bounded buffer backpressure, fault recovery watchdogs, and Dead Letter Queue management.
