package lease

import (
	"strconv"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

const (
	// maxConcurrency is the largest number of handlers one tracker runs at
	// once.
	maxConcurrency = 10000
	// maxLongPoll is the longest wait a Lease request may ask the broker for.
	maxLongPoll = 30 * time.Second
)

// Config tunes a Tracker. A zero field takes its default from WithDefaults.
type Config struct {
	// Concurrency is the number of handlers that may run at once: 1 to
	// 10000. The default is 1.
	Concurrency int
	// TTL is the lease length that the tracker asks for, and the period of
	// heartbeats is a quarter of it. It must satisfy job.ValidateTTL and be
	// at least four times Margin. The default is 30s.
	TTL time.Duration
	// Margin is how long before a lease's expiry the tracker gives up on it
	// locally. The default is 3s.
	Margin time.Duration
	// LongPoll is how long a Lease request may wait at the broker for work:
	// 0 to 30s. The default is 20s.
	LongPoll time.Duration
	// AckWindow is how long the first finished job waits for others before
	// its ack goes out. The default is 2ms.
	AckWindow time.Duration
	// AckBatch is the most items in one ack or heartbeat or release call:
	// 1 to job.MaxAckBatch. The default is 128.
	AckBatch int
	// DrainTimeout is how long Drain waits for running handlers before it
	// cancels them. The default is 25s.
	DrainTimeout time.Duration
	// AckRetries is how many times a failed ack batch is sent again before
	// its leases are given up. The default is 5.
	AckRetries int
	// AckRetryBase is the wait before the first resend of an ack batch. It
	// doubles with each further resend. The default is 100ms.
	AckRetryBase time.Duration
	// LeaseRetryBase is the ceiling of the jittered wait after the first
	// failed Lease request. It doubles with each consecutive failure. The
	// default is 100ms.
	LeaseRetryBase time.Duration
	// LeaseRetryCap is the largest ceiling of that wait. The default is 5s.
	LeaseRetryCap time.Duration
}

// WithDefaults returns c with every zero field replaced by its default. A
// field that was set, even to a value that Validate rejects, is kept.
func (c Config) WithDefaults() Config {
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.TTL == 0 {
		c.TTL = 30 * time.Second
	}
	if c.Margin == 0 {
		c.Margin = 3 * time.Second
	}
	if c.LongPoll == 0 {
		c.LongPoll = 20 * time.Second
	}
	if c.AckWindow == 0 {
		c.AckWindow = 2 * time.Millisecond
	}
	if c.AckBatch == 0 {
		c.AckBatch = job.MaxAckBatch
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 25 * time.Second
	}
	if c.AckRetries == 0 {
		c.AckRetries = 5
	}
	if c.AckRetryBase == 0 {
		c.AckRetryBase = 100 * time.Millisecond
	}
	if c.LeaseRetryBase == 0 {
		c.LeaseRetryBase = 100 * time.Millisecond
	}
	if c.LeaseRetryCap == 0 {
		c.LeaseRetryCap = 5 * time.Second
	}
	return c
}

// Validate checks c as it is, without filling defaults. The error wraps
// job.ErrInvalid and names the first field that is out of range.
func (c Config) Validate() error {
	if c.Concurrency < 1 || c.Concurrency > maxConcurrency {
		return invalid("concurrency", "must be between 1 and "+strconv.Itoa(maxConcurrency)+", got "+strconv.Itoa(c.Concurrency))
	}
	if err := job.ValidateTTL(c.TTL); err != nil {
		return err
	}
	if c.Margin < 0 {
		return invalid("margin", "must not be negative, got "+c.Margin.String())
	}
	if c.TTL < 4*c.Margin {
		return invalid("ttl", "must be at least four times the margin "+c.Margin.String()+", got "+c.TTL.String())
	}
	if c.LongPoll < 0 || c.LongPoll > maxLongPoll {
		return invalid("long_poll", "must be between 0 and "+maxLongPoll.String()+", got "+c.LongPoll.String())
	}
	if c.AckBatch < 1 || c.AckBatch > job.MaxAckBatch {
		return invalid("ack_batch", "must be between 1 and "+strconv.Itoa(job.MaxAckBatch)+", got "+strconv.Itoa(c.AckBatch))
	}
	for _, d := range []struct {
		field string
		value time.Duration
	}{
		{"ack_window", c.AckWindow},
		{"drain_timeout", c.DrainTimeout},
		{"ack_retry_base", c.AckRetryBase},
		{"lease_retry_base", c.LeaseRetryBase},
		{"lease_retry_cap", c.LeaseRetryCap},
	} {
		if d.value < 0 {
			return invalid(d.field, "must not be negative, got "+d.value.String())
		}
	}
	if c.AckRetries < 0 {
		return invalid("ack_retries", "must not be negative, got "+strconv.Itoa(c.AckRetries))
	}
	return nil
}

func invalid(field, reason string) error { return &job.InvalidError{Field: field, Reason: reason} }
