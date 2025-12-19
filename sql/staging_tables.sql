-- Example staging table for Debezium-wrapped orders payloads
CREATE TABLE IF NOT EXISTS stg_orders (
    payload JSONB NOT NULL,
    ingest_system_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _meta_topic TEXT,
    _meta_partition BIGINT,
    _meta_offset BIGINT,
    _meta_ingest_ts BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,

    -- Composite primary key for idempotent writes and deduplication
    PRIMARY KEY (_meta_topic, _meta_partition, _meta_offset),

    -- Index for time-range queries during downstream processing
    INDEX idx_stg_orders_ingest_time (ingest_system_time)
);
