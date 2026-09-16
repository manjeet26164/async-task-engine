-- ==============================================================================
-- Flyway Database Migration: V2__add_optimistic_locking_version.sql
-- Adds optimistic_version column to job_records for JPA @Version optimistic locking
-- ==============================================================================

ALTER TABLE job_records ADD COLUMN IF NOT EXISTS optimistic_version INTEGER NOT NULL DEFAULT 0;
