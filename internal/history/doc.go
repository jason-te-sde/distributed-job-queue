// Package history is the record of what happened during a chaos run, and the
// checker that decides whether the queue kept its promises.
//
// Every process of a run appends the events it witnesses to its own
// JSON-lines file through a Recorder: a client records the enqueues and
// cancels the server confirmed, a worker records when its handler started
// and ended and what the sink said to its write. After the run, ReadAll
// merges the files and Check compares the events with the final state of
// the jobs table and the sink table.
//
// The package is not part of the pure core: it reads and writes through
// io.Reader and io.Writer, and a Recorder is shared by many goroutines. It
// knows nothing about the stores or the broker, so a history can be checked
// without a database.
package history
