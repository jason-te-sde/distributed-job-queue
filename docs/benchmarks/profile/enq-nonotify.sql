\set k random(1, 2000000000)
WITH clock AS MATERIALIZED (SELECT jobqueue.now() AS now),
ins AS (
    INSERT INTO jobqueue.jobs (queue, tenant, state, payload, run_at, attempts, max_attempts, idempotency_key, created_at)
    SELECT 'probe-' || :client_id, 'bench', 'ready', '\x00112233'::bytea,
           greatest(clock.now, clock.now) + 0::int8 * interval '1 microsecond', 0, 25, :k::text, clock.now
    FROM clock
    ON CONFLICT (tenant, queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING id, queue, tenant, run_at
),
wake AS (
    SELECT 1
    FROM ins, clock
    WHERE ins.run_at <= clock.now
)
SELECT ins.id FROM ins, (SELECT count(*) FROM wake) AS woken;
