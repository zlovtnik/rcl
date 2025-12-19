-- Example staging table for Debezium-wrapped orders payloads
CREATE TABLE IF NOT EXISTS stg_orders (
    payload JSONB NOT NULL,
    ingest_system_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _meta_topic TEXT,
    _meta_partition BIGINT,
    _meta_offset BIGINT,
    _meta_ingest_ts BIGINT
);
