// Package job is the pure core of the queue: the job model, the outcomes
// of an operation, and the validation of every value that crosses a trust
// boundary.
//
// The package performs no I/O, reads no clock and keeps no state. Callers
// that need the time pass it in, so that the same code runs against
// PostgreSQL's clock in production and against a simulated clock in tests.
package job
