package job

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits on what a caller may send. They are the same in every store and at
// the broker.
const (
	// DefaultMaxAttempts is the lease budget of a job whose Spec leaves
	// MaxAttempts at 0.
	DefaultMaxAttempts int32 = 25
	// MaxMaxAttempts is the largest lease budget a Spec may ask for.
	MaxMaxAttempts int32 = 100
	// MaxPayloadBytes is the largest payload, 256 KiB.
	MaxPayloadBytes = 256 << 10
	// MaxResultBytes is the largest result, 64 KiB.
	MaxResultBytes = 64 << 10
	// MaxNameBytes is the longest queue or tenant name.
	MaxNameBytes = 128
	// MaxIdempotencyKeyBytes is the longest idempotency key.
	MaxIdempotencyKeyBytes = 255
	// MaxErrorBytes is the longest stored error message; see TruncateError.
	MaxErrorBytes = 4096
	// MaxWorkerIDBytes is the longest worker id.
	MaxWorkerIDBytes = 128
	// MaxBatch is the most items in an Enqueue, and the most jobs a Lease
	// takes in all. These carry payloads, so 64 of them stay under 16 MiB.
	MaxBatch = 64
	// MaxAckBatch is the most items in a Heartbeat, Complete, Fail or
	// Release call. It matches the 128-item ack coalescing window.
	MaxAckBatch = 128
	// MinTTL is the shortest lease.
	MinTTL = time.Second
	// MaxTTL is the longest lease.
	MaxTTL = time.Hour
	// MaxDelay is the longest enqueue delay.
	MaxDelay = 365 * 24 * time.Hour
)

// ErrInvalid is the error that every rejection of a caller's argument wraps.
var ErrInvalid = errors.New("invalid argument")

// InvalidError says which argument was rejected and why. It wraps
// ErrInvalid.
type InvalidError struct {
	// Field names the argument by its wire name, such as "queue" or
	// "max_attempts".
	Field string
	// Reason says what is wrong with it.
	Reason string
}

// Error formats the error as "invalid <Field>: <Reason>".
func (e *InvalidError) Error() string { return "invalid " + e.Field + ": " + e.Reason }

// Unwrap returns ErrInvalid.
func (e *InvalidError) Unwrap() error { return ErrInvalid }

func invalid(field, reason string) error { return &InvalidError{Field: field, Reason: reason} }

// ValidateName checks a queue or tenant name: 1 to MaxNameBytes bytes, the
// first an ASCII letter or digit, the rest letters, digits, '_', '.' or '-'.
// The check runs byte by byte rather than through a regular expression
// because it is on the enqueue path. The alphabet is strict because the
// PostgreSQL notification payload format relies on it.
func ValidateName(field, v string) error {
	if v == "" {
		return invalid(field, "must not be empty")
	}
	if len(v) > MaxNameBytes {
		return invalid(field, "must be at most "+strconv.Itoa(MaxNameBytes)+" bytes, got "+strconv.Itoa(len(v)))
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		alnum := 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9'
		if alnum || i > 0 && (c == '_' || c == '.' || c == '-') {
			continue
		}
		if i == 0 {
			return invalid(field, "must start with a letter or digit")
		}
		return invalid(field, "may contain only letters, digits, '_', '.' and '-'")
	}
	return nil
}

// ValidateSpec checks an enqueue request: both names, the payload size,
// the delay, that Delay and RunAt are not combined, MaxAttempts being 0 or
// in [1, MaxMaxAttempts], and the idempotency key.
func ValidateSpec(s Spec) error {
	if err := ValidateName("queue", s.Queue); err != nil {
		return err
	}
	if err := ValidateName("tenant", s.Tenant); err != nil {
		return err
	}
	if len(s.Payload) > MaxPayloadBytes {
		return invalid("payload", "must be at most "+strconv.Itoa(MaxPayloadBytes)+" bytes, got "+strconv.Itoa(len(s.Payload)))
	}
	if err := ValidateDelay("delay", s.Delay); err != nil {
		return err
	}
	if s.Delay > 0 && !s.RunAt.IsZero() {
		return invalid("run_at", "must not be combined with a positive delay")
	}
	if s.MaxAttempts != 0 && (s.MaxAttempts < 1 || s.MaxAttempts > MaxMaxAttempts) {
		return invalid("max_attempts", "must be 0 or between 1 and "+strconv.Itoa(int(MaxMaxAttempts))+", got "+strconv.Itoa(int(s.MaxAttempts)))
	}
	return validateIdempotencyKey(s.IdempotencyKey)
}

func validateIdempotencyKey(k string) error {
	switch {
	case len(k) > MaxIdempotencyKeyBytes:
		return invalid("idempotency_key", "must be at most "+strconv.Itoa(MaxIdempotencyKeyBytes)+" bytes, got "+strconv.Itoa(len(k)))
	case !utf8.ValidString(k):
		return invalid("idempotency_key", "must be valid UTF-8")
	case strings.IndexByte(k, 0) >= 0:
		return invalid("idempotency_key", "must not contain NUL")
	}
	return nil
}

// ValidateTTL checks a lease length: within [MinTTL, MaxTTL] and a whole
// number of microseconds, the precision the database stores.
func ValidateTTL(d time.Duration) error {
	if d < MinTTL || d > MaxTTL {
		return invalid("ttl", "must be between "+MinTTL.String()+" and "+MaxTTL.String()+", got "+d.String())
	}
	if d%time.Microsecond != 0 {
		return invalid("ttl", "must be a whole number of microseconds, got "+d.String())
	}
	return nil
}

// ValidateDelay checks a delay that the caller names field: within
// [0, MaxDelay] and a whole number of microseconds.
func ValidateDelay(field string, d time.Duration) error {
	if d < 0 || d > MaxDelay {
		return invalid(field, "must be between 0 and "+MaxDelay.String()+", got "+d.String())
	}
	if d%time.Microsecond != 0 {
		return invalid(field, "must be a whole number of microseconds, got "+d.String())
	}
	return nil
}

// ValidateResult checks the result a worker returns: at most MaxResultBytes.
func ValidateResult(b []byte) error {
	if len(b) > MaxResultBytes {
		return invalid("result", "must be at most "+strconv.Itoa(MaxResultBytes)+" bytes, got "+strconv.Itoa(len(b)))
	}
	return nil
}

// ValidateWorkerID checks a worker id: 1 to MaxWorkerIDBytes bytes, each
// printable ASCII without the space (0x21 to 0x7e).
func ValidateWorkerID(s string) error {
	if s == "" {
		return invalid("worker_id", "must not be empty")
	}
	if len(s) > MaxWorkerIDBytes {
		return invalid("worker_id", "must be at most "+strconv.Itoa(MaxWorkerIDBytes)+" bytes, got "+strconv.Itoa(len(s)))
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return invalid("worker_id", "must be printable ASCII without spaces")
		}
	}
	return nil
}

// ValidateBatch checks that a request named field carries between 1 and
// limit items.
func ValidateBatch(field string, n, limit int) error {
	if n < 1 || n > limit {
		return invalid(field, "must hold between 1 and "+strconv.Itoa(limit)+" items, got "+strconv.Itoa(n))
	}
	return nil
}

// TruncateError makes an error message storable: invalid UTF-8 becomes
// U+FFFD, NUL is removed, and the text is cut to at most MaxErrorBytes bytes
// without splitting a character.
func TruncateError(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) <= MaxErrorBytes {
		return s
	}
	cut := MaxErrorBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
