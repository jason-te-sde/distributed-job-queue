package pgstore

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// jobCols lists the columns of a job in the order scanJob reads them. The
// statements that return whole rows name the table j.
const jobCols = `j.id, j.queue, j.tenant, j.state, j.payload, j.run_at, j.attempts, j.max_attempts, ` +
	`j.lease_epoch, j.lease_expires_at, j.leased_by, j.idempotency_key, j.last_error, j.result, ` +
	`j.created_at, j.finished_at`

// scanJob reads one row of jobCols, followed by the values that extra points
// to. NULL becomes the zero time or the empty string, and every time is
// normalized. pgx hands out fresh memory for the byte slices, so the job
// shares nothing with the connection.
func scanJob(row pgx.Row, extra ...any) (job.Job, error) {
	var (
		j                      job.Job
		state                  string
		runAt, createdAt       time.Time
		leaseExpiresAt         *time.Time
		finishedAt             *time.Time
		leasedBy, key, lastErr *string
	)
	dest := append([]any{
		&j.ID, &j.Queue, &j.Tenant, &state, &j.Payload, &runAt, &j.Attempts, &j.MaxAttempts,
		&j.Epoch, &leaseExpiresAt, &leasedBy, &key, &lastErr, &j.Result,
		&createdAt, &finishedAt,
	}, extra...)
	if err := row.Scan(dest...); err != nil {
		return job.Job{}, err
	}
	var err error
	if j.State, err = job.ParseState(state); err != nil {
		return job.Job{}, fmt.Errorf("job %d: %w", j.ID, err)
	}
	j.RunAt = job.NormalizeTime(runAt)
	j.CreatedAt = job.NormalizeTime(createdAt)
	if leaseExpiresAt != nil {
		j.LeaseExpiresAt = job.NormalizeTime(*leaseExpiresAt)
	}
	if finishedAt != nil {
		j.FinishedAt = job.NormalizeTime(*finishedAt)
	}
	j.LeasedBy = deref(leasedBy)
	j.IdempotencyKey = deref(key)
	j.LastError = deref(lastErr)
	return j, nil
}

// deref returns the string that p points to, or "" for NULL.
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// nullable returns nil for the empty string and s otherwise, which the
// database stores as NULL.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// duration converts the difference of two timestamps. PostgreSQL gives it as
// microseconds plus whole days, never months.
func duration(iv pgtype.Interval) time.Duration {
	return time.Duration(iv.Microseconds)*time.Microsecond + time.Duration(iv.Days)*24*time.Hour
}

// micros is how a duration crosses into SQL: whole microseconds in an int8.
// Every caller has validated that d is a whole number of microseconds.
func micros(d time.Duration) int64 { return d.Microseconds() }
