-- ==============================================================================
-- Flyway Database Migration: V1__init_schema.sql
-- Recreates the job_records table schema with lease management & idempotency support
-- ==============================================================================

CREATE TABLE IF NOT EXISTS job_records (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    idempotency_key VARCHAR(255) NOT NULL,
    task_type VARCHAR(255) NOT NULL,
    payload TEXT,
    status VARCHAR(255) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    worker_id VARCHAR(255),
    lease_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT idx_job_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_job_records_status ON job_records(status);
CREATE INDEX IF NOT EXISTS idx_job_records_status_updated_at ON job_records(status, updated_at);
