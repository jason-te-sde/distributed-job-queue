package history

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Kind says what an Event witnessed.
type Kind string

// The kinds of event. The comment on each lists the Event fields it uses.
const (
	// EnqueueConfirmed is an enqueue the server acknowledged: Key, ID.
	EnqueueConfirmed Kind = "enqueue_confirmed"
	// CancelConfirmed is a cancel the server acknowledged: ID.
	CancelConfirmed Kind = "cancel_confirmed"
	// HandlerStart is a handler beginning to run a delivery: ID, Token,
	// Worker.
	HandlerStart Kind = "handler_start"
	// HandlerEnd is a handler returning: ID, Token, Worker, Result ("ok" or
	// "error").
	HandlerEnd Kind = "handler_end"
	// SinkWrite is a handler's write to the sink table: ID, Token, Worker,
	// Applied (the write changed exactly one row).
	SinkWrite Kind = "sink_write"
)

// valid reports whether k is one of the five kinds.
func (k Kind) valid() bool {
	switch k {
	case EnqueueConfirmed, CancelConfirmed, HandlerStart, HandlerEnd, SinkWrite:
		return true
	}
	return false
}

// Event is one line of a history file. The comment on each Kind says which
// fields it uses; the others stay zero and are left out of the JSON.
type Event struct {
	// At is when the event happened, as the recording process saw it. Check
	// orders the sink writes of one job by At, so the caller sets it, from
	// a clock that every process of the run shares.
	At time.Time `json:"at"`
	// Kind says what happened.
	Kind Kind `json:"kind"`
	// Key is the idempotency key of the enqueue.
	Key string `json:"key,omitempty"`
	// ID is the job's id.
	ID int64 `json:"id,omitempty"`
	// Token is the fencing token (lease epoch) of the delivery.
	Token int64 `json:"token,omitempty"`
	// Worker is the id of the worker that ran the delivery.
	Worker string `json:"worker,omitempty"`
	// Result is how the handler ended: "ok" or "error".
	Result string `json:"result,omitempty"`
	// Applied reports that the sink write changed exactly one row, that is,
	// that the sink accepted the token.
	Applied bool `json:"applied,omitempty"`
}

// Recorder appends events to a writer as JSON lines. It is safe for
// concurrent use: many handler goroutines record into one file.
type Recorder struct {
	mu  sync.Mutex
	bw  *bufio.Writer
	enc *json.Encoder
}

// NewRecorder returns a Recorder that writes to w. The caller keeps
// ownership of w and closes it; the Recorder holds nothing back, so nothing
// is lost if the process dies after a Record call returned.
func NewRecorder(w io.Writer) *Recorder {
	bw := bufio.NewWriter(w)
	return &Recorder{bw: bw, enc: json.NewEncoder(bw)}
}

// Record writes e as one line and flushes it to the underlying writer before
// it returns. It refuses an event whose Kind is not one of the five, because
// ReadAll would refuse the line. After a write error every later call
// returns an error too.
func (r *Recorder) Record(e Event) error {
	if !e.Kind.valid() {
		return fmt.Errorf("history: cannot record an event of unknown kind %q", e.Kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Encode(e); err != nil {
		return fmt.Errorf("history: encode %s event: %w", e.Kind, err)
	}
	if err := r.bw.Flush(); err != nil {
		return fmt.Errorf("history: flush %s event: %w", e.Kind, err)
	}
	return nil
}

// ReadAll reads the JSON-lines files rs, one reader per file, and returns
// their events concatenated in argument order. A line that is not a single
// JSON event of a known kind, blank lines included, is an error that names
// the file by its index in rs (from 0) and the line by its number (from 1).
// A read error is reported the same way. On error no events are returned.
func ReadAll(rs ...io.Reader) ([]Event, error) {
	var events []Event
	for i, r := range rs {
		br := bufio.NewReader(r)
		for line := 1; ; line++ {
			text, err := br.ReadBytes('\n')
			atEOF := errors.Is(err, io.EOF)
			if err != nil && !atEOF {
				return nil, fmt.Errorf("history: file index %d, line %d: %w", i, line, err)
			}
			if len(text) > 0 {
				e, derr := decodeLine(text)
				if derr != nil {
					return nil, fmt.Errorf("history: file index %d, line %d: %w", i, line, derr)
				}
				events = append(events, e)
			}
			if atEOF {
				break
			}
		}
	}
	return events, nil
}

// decodeLine parses one line as exactly one Event of a known kind.
func decodeLine(line []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(line, &e); err != nil {
		return Event{}, err
	}
	if !e.Kind.valid() {
		return Event{}, fmt.Errorf("unknown event kind %q", e.Kind)
	}
	return e, nil
}
