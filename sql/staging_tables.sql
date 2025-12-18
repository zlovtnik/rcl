-- Example staging table for Debezium-wrapped orders payloads
CREATE TABLE IF NOT EXISTS stg_orders (
    payload JSONB NOT NULL,
    ingest_system_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _meta_topic TEXT,
    _meta_partition BIGINT,
    _meta_offset BIGINT,
    _meta_ingest_ts BIGINT,
    order_id TEXT GENERATED ALWAYS AS (payload ->> 'order_id') STORED,
    op_ts TIMESTAMPTZ GENERATED ALWAYS AS ((payload ->> 'op_ts')::timestamptz) STORED,
    operation_type TEXT GENERATED ALWAYS AS (payload ->> 'operation_type') STORED,
    CHECK (payload ? 'order_id'),
    CHECK (payload ? 'op_ts'),
    CONSTRAINT stg_orders_op_ts_valid CHECK (
        (payload ->> 'op_ts') IS NOT NULL
        AND (payload ->> 'op_ts') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$'
    ),
    CHECK (payload ? 'operation_type'),
    PRIMARY KEY (order_id, op_ts, operation_type)
);
