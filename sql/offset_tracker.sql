-- Offset tracking table for exactly-once semantics with multi-tenancy support
-- Stores the last committed Kafka offset for each partition per pipeline per tenant
-- Used for exactly-once processing: on startup, seek consumer to stored offsets
-- On successful write, update offset in same transaction as data write
CREATE TABLE IF NOT EXISTS offset_tracker (
    tenant_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, pipeline_name, topic, partition)
);

-- Index for efficient lookups by pipeline
CREATE INDEX IF NOT EXISTS idx_offset_tracker_pipeline
    ON offset_tracker(tenant_id, pipeline_name);

-- Index for efficient lookups by topic (for bulk recovery)
CREATE INDEX IF NOT EXISTS idx_offset_tracker_topic
    ON offset_tracker(tenant_id, topic);
