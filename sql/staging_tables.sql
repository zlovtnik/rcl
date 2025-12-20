-- Example staging table for Debezium-wrapped orders payloads with multi-tenancy support
CREATE TABLE IF NOT EXISTS stg_orders (
    tenant_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    ingest_system_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _meta_topic TEXT NOT NULL,
    _meta_partition BIGINT NOT NULL,
    _meta_offset BIGINT NOT NULL,
    _meta_ingest_ts BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,

    -- Composite primary key for idempotent writes and deduplication (includes tenant_id)
    PRIMARY KEY (tenant_id, _meta_topic, _meta_partition, _meta_offset)
);

-- Index for time-range queries during downstream processing
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stg_orders_ingest_time ON stg_orders (ingest_system_time);

-- Index for tenant-specific queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stg_orders_tenant_id ON stg_orders (tenant_id);
