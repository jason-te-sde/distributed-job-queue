-- 0001_init: the clock function and the jobs table with its constraints and
-- indexes. The jobqueue schema and schema_migrations already exist: Up creates
-- them before it applies the first migration.

-- The database clock is the only time authority. Every expiry and run_at
-- comparison goes through this function, evaluated once per statement, so
-- that the Postgres tests can replace it with a controllable clock
-- (internal/pgtest) without touching a query.
CREATE FUNCTION jobqueue.now() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT clock_timestamp() $$;

CREATE TABLE jobqueue.jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue text NOT NULL,
    tenant text NOT NULL,
    state text NOT NULL CHECK (state IN ('ready', 'leased', 'succeeded', 'dead', 'cancelled')),
    -- 262144 bytes (256 KiB): the payload limit, enforced again here so that no
    -- writer can bypass it.
    payload bytea NOT NULL CHECK (octet_length(payload) <= 262144),
    run_at timestamptz NOT NULL,
    attempts int NOT NULL CHECK (attempts >= 0),
    max_attempts int NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    -- The fencing token: it grows by one on every lease and never repeats.
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    leased_by text,
    idempotency_key text,
    last_error text,
    -- 65536 bytes (64 KiB): the result limit.
    result bytea CHECK (result IS NULL OR octet_length(result) <= 65536),
    created_at timestamptz NOT NULL,
    finished_at timestamptz,

    CONSTRAINT jobs_attempts_within_max CHECK (attempts <= max_attempts),
    -- A lease exists exactly while the job is leased.
    CONSTRAINT jobs_leased_iff_lease_set
        CHECK ((state = 'leased') = (lease_expires_at IS NOT NULL AND leased_by IS NOT NULL)),
    -- finished_at is set exactly in the three terminal states.
    CONSTRAINT jobs_finished_iff_terminal
        CHECK ((state IN ('succeeded', 'dead', 'cancelled')) = (finished_at IS NOT NULL))
) WITH (
    -- fillfactor 85 (default 100). Every transition on the hot path (lease,
    -- heartbeat, complete, fail) changes state, lease_expires_at or run_at, each
    -- a key or predicate column of a partial index below, so none of them can be a
    -- HOT update; measured on PostgreSQL 17, 0 of 1500 such updates were HOT. The
    -- headroom therefore does not buy HOT chains. It keeps a row's next version on
    -- its predecessor's page and makes opportunistic page pruning start earlier
    -- (it runs once a page has less free space than the headroom), which reclaims
    -- dead versions between vacuums. Measured on PostgreSQL 17 with a vacuum every
    -- 5 rounds of lease, 2 heartbeats, complete and delete over 5000 live rows,
    -- the heap settled at 3191 pages with fillfactor 100, 2704 with 85 and 1851
    -- with 70. 85 rather than 70, because finished rows stay for hours or days and
    -- every page holding them keeps the headroom unused. The soak (E5) compares 70,
    -- 85 and 100 under the real mix.
    fillfactor = 85,

    -- Autovacuum: every job leaves at least two dead row versions (one when it
    -- is leased, one when it completes) plus one per heartbeat, so dead tuples
    -- accumulate faster than rows. The defaults (20% of the table plus 50 rows)
    -- would let a table of millions of retained rows carry hundreds of thousands
    -- of dead tuples, bloating the partial indexes that lease scans.
    -- autovacuum_naptime is a server-wide setting that bounds how soon a due
    -- vacuum can start; no per-table value changes it.
    autovacuum_vacuum_scale_factor = 0.02,
    -- A base of 1000 rows keeps a nearly empty table, which the scale factor alone
    -- would vacuum after a handful of updates, from being vacuumed on every pass.
    autovacuum_vacuum_threshold = 1000,
    -- The planner's choice between the ready indexes and a scan depends on how
    -- many rows are ready, which swings from a burst to a drained queue within
    -- seconds; analyze after 2% of the rows changed so the statistics follow.
    autovacuum_analyze_scale_factor = 0.02,
    -- Payloads up to 256 KiB live in the TOAST table and are freed only when prune
    -- deletes the row. At the default 20% a table of large payloads would hold a
    -- fifth of its size as dead space before vacuum touches it.
    toast.autovacuum_vacuum_scale_factor = 0.05
);

-- Lease, FIFO within a queue and DRR across tenants read only ready rows, so both
-- ready indexes are partial. Both order by (run_at, id): the first row that is due
-- is the oldest, and id breaks ties deterministically.
-- jobs_ready_tenant serves a lease for one tenant.
CREATE INDEX jobs_ready_tenant ON jobqueue.jobs (queue, tenant, run_at, id) WHERE state = 'ready';
-- jobs_ready_fifo serves a lease that does not name a tenant.
CREATE INDEX jobs_ready_fifo ON jobqueue.jobs (queue, run_at, id) WHERE state = 'ready';

-- The reaper finds expired leases by scanning this index from the left, oldest
-- expiry first.
CREATE INDEX jobs_leased_expiry ON jobqueue.jobs (lease_expires_at, id) WHERE state = 'leased';

-- Idempotency is scoped to (tenant, queue), and only jobs that carry a key take
-- part: the store maps an empty key to NULL.
CREATE UNIQUE INDEX jobs_idempotency ON jobqueue.jobs (tenant, queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Prune deletes finished rows oldest first once they pass their retention.
CREATE INDEX jobs_finished ON jobqueue.jobs (finished_at, id) WHERE finished_at IS NOT NULL;

-- Listing and requeueing the dead letters of a queue.
CREATE INDEX jobs_dead ON jobqueue.jobs (queue, id) WHERE state = 'dead';
