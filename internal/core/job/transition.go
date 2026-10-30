package job

import "time"

// The functions in this file are the transition table of a job. Each takes
// the job by value and returns the job after the operation together with its
// Outcome; unless the Outcome is Applied the job comes back unchanged. None
// of them reads a clock: now is the caller's, and the stores take it from
// the database, so that every comparison here is made against one time
// authority.
//
// Callers pass now already normalized (NormalizeTime) and durations that
// are whole microseconds (ValidateTTL, ValidateDelay), so that every time
// stored in a Job keeps the form the Job documents.

// New builds the job that an accepted Spec becomes. The Spec must have
// passed ValidateSpec. The job is ready; it runs at now plus Delay, or, when
// the Spec names a RunAt, at that instant or at now if the instant has
// passed. A MaxAttempts of 0 becomes DefaultMaxAttempts. The times in the
// result are normalized.
func New(s Spec, id int64, now time.Time) Job {
	now = NormalizeTime(now)
	runAt := now.Add(s.Delay)
	if !s.RunAt.IsZero() {
		runAt = now
		if at := NormalizeTime(s.RunAt); at.After(now) {
			runAt = at
		}
	}
	maxAttempts := s.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	return Job{
		ID:             id,
		Queue:          s.Queue,
		Tenant:         s.Tenant,
		State:          StateReady,
		Payload:        s.Payload,
		RunAt:          runAt,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: s.IdempotencyKey,
		CreatedAt:      now,
	}
}

// holds reports whether token is the fencing token of j's live lease: j is
// leased and its epoch is the token.
func holds(j Job, token int64) bool { return j.State == StateLeased && j.Epoch == token }

// clearLease drops the fields that only a leased job has. Epoch stays: it
// is the fencing token of the last lease.
func clearLease(j Job) Job {
	j.LeaseExpiresAt = time.Time{}
	j.LeasedBy = ""
	return j
}

// Lease hands a ready job to worker for ttl. It is WrongState unless the job
// is ready and its RunAt has come (RunAt <= now). Applying it makes the job
// leased, adds one to Epoch, which becomes the worker's fencing token, and
// uses one attempt.
func Lease(j Job, now time.Time, ttl time.Duration, worker string) (Job, Outcome) {
	if j.State != StateReady || j.RunAt.After(now) {
		return j, WrongState
	}
	j.State = StateLeased
	j.Epoch++
	j.Attempts++
	j.LeaseExpiresAt = now.Add(ttl)
	j.LeasedBy = worker
	return j, Applied
}

// Heartbeat extends the lease that token names to now plus ttl. It is Fenced
// unless the job is leased under that token, and Expired when the lease has
// already run out (LeaseExpiresAt <= now): a lease that has run out cannot
// be revived, because the reaper may take the job at any moment.
func Heartbeat(j Job, token int64, now time.Time, ttl time.Duration) (Job, Outcome) {
	if !holds(j, token) {
		return j, Fenced
	}
	if !j.LeaseExpiresAt.After(now) {
		return j, Expired
	}
	j.LeaseExpiresAt = now.Add(ttl)
	return j, Applied
}

// Complete records that the worker holding token finished the job with
// result. The lease's expiry is ignored: finished work is kept as long as
// nobody else has leased the job since. Completing a job that is already
// succeeded under the same token is a Duplicate and keeps the first result.
// Every other call is Fenced.
func Complete(j Job, token int64, now time.Time, result []byte) (Job, Outcome) {
	switch {
	case holds(j, token):
		j = clearLease(j)
		j.State = StateSucceeded
		j.Result = result
		j.FinishedAt = now
		return j, Applied
	case j.State == StateSucceeded && j.Epoch == token:
		return j, Duplicate
	}
	return j, Fenced
}

// Fail records that the worker holding token failed the job with errMsg,
// stored through TruncateError. The job becomes ready again, to run after
// delay, while it has attempts left and the failure is not permanent;
// otherwise it becomes dead. It is Fenced unless the job is leased under
// token.
func Fail(j Job, token int64, now time.Time, permanent bool, delay time.Duration, errMsg string) (Job, Outcome) {
	if !holds(j, token) {
		return j, Fenced
	}
	j = clearLease(j)
	j.LastError = TruncateError(errMsg)
	if !permanent && j.Attempts < j.MaxAttempts {
		j.State = StateReady
		j.RunAt = now.Add(delay)
	} else {
		j.State = StateDead
		j.FinishedAt = now
	}
	return j, Applied
}

// Release gives a leased job back unrun: the worker holding token had not
// started it, so the attempt is refunded and the job is ready at once. It is
// Fenced unless the job is leased under token.
func Release(j Job, token int64, now time.Time) (Job, Outcome) {
	if !holds(j, token) {
		return j, Fenced
	}
	j = clearLease(j)
	j.State = StateReady
	j.RunAt = now
	j.Attempts--
	return j, Applied
}

// Cancel stops a job that has not finished. A ready or leased job becomes
// cancelled, and a worker still holding its lease is fenced from then on.
// Cancelling a cancelled job is a Duplicate; a succeeded or dead job is
// WrongState.
func Cancel(j Job, now time.Time) (Job, Outcome) {
	switch j.State {
	case StateReady, StateLeased:
		j = clearLease(j)
		j.State = StateCancelled
		j.FinishedAt = now
		return j, Applied
	case StateCancelled:
		return j, Duplicate
	case StateSucceeded, StateDead:
		return j, WrongState
	}
	return j, WrongState
}

// Reap takes back a lease that has run out (LeaseExpiresAt <= now) as a
// retryable failure of the attempt, recorded as "lease expired": the job is
// ready again after delay while it has attempts left, and dead otherwise.
// Any other job is WrongState.
func Reap(j Job, now time.Time, delay time.Duration) (Job, Outcome) {
	if j.State != StateLeased || j.LeaseExpiresAt.After(now) {
		return j, WrongState
	}
	return Fail(j, j.Epoch, now, false, delay, "lease expired")
}

// Requeue revives a dead job for another run at now with a fresh attempt
// budget. LastError stays, so that the reason for the earlier death remains
// visible; Result and FinishedAt are cleared. Any other job is WrongState.
func Requeue(j Job, now time.Time) (Job, Outcome) {
	if j.State != StateDead {
		return j, WrongState
	}
	j.State = StateReady
	j.Attempts = 0
	j.RunAt = now
	j.FinishedAt = time.Time{}
	j.Result = nil
	return j, Applied
}

// Prunable reports whether a finished job has outlived its retention at now:
// a succeeded or cancelled job once more than done has passed since
// FinishedAt, a dead job once more than dead has. Exactly at the retention
// the job is kept. A job that has not finished is never prunable.
func Prunable(j Job, now time.Time, done, dead time.Duration) bool {
	switch j.State {
	case StateSucceeded, StateCancelled:
		return now.Sub(j.FinishedAt) > done
	case StateDead:
		return now.Sub(j.FinishedAt) > dead
	case StateReady, StateLeased:
		return false
	}
	return false
}
