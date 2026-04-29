-- Audit log: one row per partition successfully archived to S3. The unique
-- constraint on partition_name doubles as an idempotency guard against
-- concurrent or retried archive runs.
CREATE TABLE archive_log (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partition_name  TEXT         NOT NULL UNIQUE,
    s3_bucket       TEXT         NOT NULL,
    s3_key          TEXT         NOT NULL,
    row_count       BIGINT       NOT NULL,
    bytes_uploaded  BIGINT       NOT NULL,
    archived_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
