package broker

import (
	"testing"
	"time"
)

func TestReconnectDelayIsFullJitterUpToTheCap(t *testing.T) {
	// The ceiling doubles from 100ms to 5s, and the delay is a uniform draw
	// below it.
	for _, tc := range []struct {
		attempt int
		u       float64
		want    time.Duration
	}{
		{0, 0, 0},
		{0, 0.5, 50 * time.Millisecond},
		{1, 0.5, 100 * time.Millisecond},
		{3, 0.5, 400 * time.Millisecond},
		{6, 0.5, 2500 * time.Millisecond},  // 6.4s would be the ceiling, capped to 5s
		{60, 0.5, 2500 * time.Millisecond}, // no overflow for a long outage
		{1 << 30, 0.5, 2500 * time.Millisecond},
	} {
		if got := reconnectDelay(tc.attempt, tc.u); got != tc.want {
			t.Errorf("reconnectDelay(%d, %v) = %v, want %v", tc.attempt, tc.u, got, tc.want)
		}
	}
	if got := reconnectDelay(10, 0.999999); got >= 5*time.Second {
		t.Errorf("reconnectDelay(10, 0.999999) = %v, want below the 5s cap", got)
	}
}
