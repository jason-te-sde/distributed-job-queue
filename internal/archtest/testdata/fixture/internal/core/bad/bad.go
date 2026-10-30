// Package bad breaks every core rule on purpose; archtest loads it as a fixture.
package bad

import (
	"math/rand/v2"
	"os"
	"time"
)

// Break uses a go statement, a select statement, a channel type, the clock,
// global generator functions and a banned import. Seeded generators are fine.
func Break(done <-chan struct{}) int {
	go func() {}()
	select {
	case <-done:
	default:
	}
	_ = make(chan int)
	_ = time.Now()
	_ = os.Getenv("HOME")
	var t0 time.Time
	_ = t0.Sub(t0) + time.Duration(len(os.Args))
	r := rand.New(rand.NewPCG(1, 2))
	return rand.IntN(3) + rand.N[int](3) + r.IntN(3)
}
