-- ==============================================================================
-- OPTIO REPLICATION PLATFORM - RELATIONAL DATABASE SCHEMA (v1.0)
-- Target Database: PostgreSQL 16+
-- Modules: Source Data, Checkpoint Watermarks, Dead Letter Queue (DLQ)
-- ==============================================================================

-- Enable pgcrypto extension for gen_random_uuid() if not standard
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ------------------------------------------------------------------------------
-- 1. Table: source_records
-- Primary transactional dataset replicated concurrently to Elasticsearch & RabbitMQ.
-- Engineered for O(1) memory cursor streaming and keyset pagination.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_records (
    id BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'tenant_default',
    payload JSONB NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    is_corrupted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ(3) NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ(3) NOT NULL DEFAULT NOW()
);

-- Index: B-tree on id ASC for O(1) memory keyset seek queries (Backfill Cursor)
CREATE INDEX IF NOT EXISTS idx_source_records_backfill_cursor
    ON source_records (id ASC);

-- Composite Index: B-tree on (updated_at ASC, id ASC) for CDC Watermark Seeking
CREATE INDEX IF NOT EXISTS idx_source_records_incremental_watermark
    ON source_records (updated_at ASC, id ASC);

-- Index: Multi-tenancy filtering
CREATE INDEX IF NOT EXISTS idx_source_records_tenant
    ON source_records (tenant_id);

-- Unique Index: Explicit UUID index for business lookup
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_records_uuid
    ON source_records (uuid);


-- ------------------------------------------------------------------------------
-- 2. Table: replication_checkpoints
-- Atomic watermark & operational state persistence (Gate 1: Crash Recovery).
-- Watermarks are committed strictly AFTER dual-sink acknowledgment.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS replication_checkpoints (
    pipeline_id VARCHAR(64) PRIMARY KEY,
    last_processed_id BIGINT NOT NULL DEFAULT 0,
    last_processed_timestamp TIMESTAMPTZ(3) NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'INITIALIZED',
    records_processed BIGINT NOT NULL DEFAULT 0,
    records_failed BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ(3) NOT NULL DEFAULT NOW()
);

-- Seed Initial Pipeline Checkpoint Records (Idempotent)
INSERT INTO replication_checkpoints (
    pipeline_id,
    last_processed_id,
    last_processed_timestamp,
    status,
    records_processed,
    records_failed,
    metadata,
    updated_at
)
VALUES
    (
        'backfill_pipeline',
        0,
        NULL,
        'INITIALIZED',
        0,
        0,
        '{"mode": "batch_keyset", "description": "Historical bulk backfill stream"}'::jsonb,
        NOW()
    ),
    (
        'incremental_pipeline',
        0,
        NOW(),
        'INITIALIZED',
        0,
        0,
        '{"mode": "cdc_watermark", "description": "Continuous incremental change stream"}'::jsonb,
        NOW()
    )
ON CONFLICT (pipeline_id) DO NOTHING;


-- ------------------------------------------------------------------------------
-- 3. Table: dead_letter_queue
-- Persistent transactional isolation of malformed or rejected records (Gate 4).
-- Preserves high pipeline throughput by rejecting poison pills without failing batches.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT NULL,
    record_uuid UUID NULL,
    sink_target VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    stack_trace TEXT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ(3) NOT NULL DEFAULT NOW(),
    last_retried_at TIMESTAMPTZ(3) NULL
);

-- Composite Index: Efficient retrieval and re-drive ordered by age
CREATE INDEX IF NOT EXISTS idx_dlq_status_created
    ON dead_letter_queue (status, created_at ASC);

-- Index: Fast lookup by offending source record ID
CREATE INDEX IF NOT EXISTS idx_dlq_record_id
    ON dead_letter_queue (record_id);
