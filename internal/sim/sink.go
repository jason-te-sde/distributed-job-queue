package sim

// Sink is the downstream system that a job's handler writes to, and that
// fences stale writers with the job's token. A write is accepted only if its
// token is not below the highest one the sink has accepted for that job, so a
// worker that lost its lease cannot overwrite what the new holder wrote.
//
// A write is accepted exactly when it raises or repeats the highest token, so
// the last accepted token of a job is also the highest seen.
type Sink struct {
	last     map[int64]int64
	rejected int
}

// NewSink returns a sink that has accepted nothing.
func NewSink() *Sink {
	return &Sink{last: map[int64]int64{}}
}

// Write records a write for job id under token, and reports whether the sink
// accepted it: true if token is at least the highest token accepted for id.
func (s *Sink) Write(id, token int64) bool {
	if highest, ok := s.last[id]; ok && token < highest {
		s.rejected++
		return false
	}
	s.last[id] = token
	return true
}

// Last returns the token of the last write accepted for job id, and false if
// none was.
func (s *Sink) Last(id int64) (int64, bool) {
	token, ok := s.last[id]
	return token, ok
}

// Rejections returns how many writes the sink has refused as stale.
func (s *Sink) Rejections() int {
	return s.rejected
}
