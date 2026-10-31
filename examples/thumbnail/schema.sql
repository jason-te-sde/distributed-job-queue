-- The table the thumbnail worker writes to. It holds one row per job, and the
-- fencing token of the lease that wrote the row: a write under a lower or an
-- equal token is refused (see sink.go). The statements are idempotent, so
-- every replica applies them at start.
CREATE TABLE IF NOT EXISTS thumbnails (
    job_id     bigint      PRIMARY KEY,
    token      bigint      NOT NULL,
    sha256     text        NOT NULL,
    bytes      bigint      NOT NULL,
    written_at timestamptz NOT NULL
);
