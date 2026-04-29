-- Parent table, range-partitioned by created_at (daily children managed by pg_partman).
-- Partition key MUST appear in the primary key for native range partitioning, hence (id, created_at).
CREATE TABLE events (
    id          BIGINT       GENERATED ALWAYS AS IDENTITY,
    tenant_id   UUID         NOT NULL,
    event_type  TEXT         NOT NULL,
    payload     JSONB        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX events_tenant_created_idx
    ON events (tenant_id, created_at DESC);
