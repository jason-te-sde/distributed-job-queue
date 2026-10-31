// Package load is an open-loop load generator.
//
// A closed-loop generator sends the next request when the previous one is
// answered, so a stalled system slows the generator down and the requests that
// should have been sent during the stall are never sent: the stall disappears
// from the measurement. This is coordinated omission. Here the time at which job
// k is to be sent is fixed in advance by the schedule, and the latency of the job
// is measured from that intended time, never from the moment the send actually
// began.
package load

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// lateAfter is how far behind its intended time a send may begin before it
	// counts as late.
	lateAfter = time.Millisecond
	// maxInFlight bounds the sends that run at once. It is far above what a
	// healthy system needs at the rates of the experiments, so reaching it
	// means the system is overloaded: the schedule then falls behind, the sends
	// count as late, and the latency measured from their intended times shows
	// it.
	maxInFlight = 4096
)

// Schedule fixes when each job is to be sent: job k at Start + k/Rate.
type Schedule struct {
	// Start is the intended time of job 0.
	Start time.Time
	// Rate is the number of jobs per second, above zero.
	Rate float64
}

// Intended returns the time at which job k is to be sent. It is computed from
// k, not accumulated, so it does not drift however many jobs have been sent. It
// panics if Rate is not above zero and finite: a schedule without a rate is a
// programming error, and Run reports it as an error before it asks.
func (s Schedule) Intended(k int) time.Time {
	if !validRate(s.Rate) {
		panic(fmt.Sprintf("load.Schedule.Intended: the rate must be above zero and finite, got %v", s.Rate))
	}
	return s.Start.Add(time.Duration(math.Round(float64(k) * float64(time.Second) / s.Rate)))
}

func validRate(r float64) bool { return r > 0 && !math.IsInf(r, 0) && !math.IsNaN(r) }

// SendFunc sends job k. intended is the time the schedule gave the job, the
// instant to measure its latency from. The call may block for as long as the
// send takes: Run does not wait for it before it starts the next.
type SendFunc func(ctx context.Context, k int, intended time.Time) error

// Run sends n jobs on the schedule, each on a goroutine of its own, at its
// intended time or as soon after as possible, and returns when every send has
// returned. sent is the number of sends that returned nil, and late the number
// that began more than 1ms behind the intended time.
//
// The first error from a send, or the end of ctx, stops the schedule: no more
// jobs are started, the ones running are waited for, and the error is returned
// with the counts. At most 4096 sends run at once, so a system that stops
// answering makes the generator fall behind, which the late count and the
// latencies measured from the intended times report, instead of growing without
// bound.
func Run(ctx context.Context, s Schedule, n int, send SendFunc) (sent, late int, err error) {
	if !validRate(s.Rate) {
		return 0, 0, fmt.Errorf("load: the rate must be above zero and finite, got %v", s.Rate)
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("load: the number of jobs must not be negative, got %d", n)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		wg      sync.WaitGroup
		sentN   atomic.Int64
		lateN   int
		slots   = make(chan struct{}, maxInFlight)
		timer   = time.NewTimer(time.Hour)
		failure sync.Once
		sendErr error
	)
	defer timer.Stop()

schedule:
	for k := 0; k < n; k++ {
		intended := s.Intended(k)
		if wait := time.Until(intended); wait > 0 {
			timer.Reset(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				break schedule
			}
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break schedule
		}
		if ctx.Err() != nil {
			break
		}
		if time.Since(intended) > lateAfter {
			lateN++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := send(ctx, k, intended); err != nil {
				failure.Do(func() {
					sendErr = fmt.Errorf("load: send of job %d: %w", k, err)
					cancel(sendErr)
				})
				return
			}
			sentN.Add(1)
		}()
	}
	wg.Wait()

	if sendErr != nil {
		return int(sentN.Load()), lateN, sendErr
	}
	if cause := context.Cause(ctx); cause != nil && int(sentN.Load()) < n {
		return int(sentN.Load()), lateN, cause
	}
	return int(sentN.Load()), lateN, nil
}
