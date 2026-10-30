package job

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// wantInvalid fails unless err is an *InvalidError that wraps ErrInvalid and
// names field.
func wantInvalid(t *testing.T, err error, field string) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want an error wrapping ErrInvalid", err)
	}
	var ie *InvalidError
	if !errors.As(err, &ie) {
		t.Fatalf("got %T, want *InvalidError", err)
	}
	if ie.Field != field {
		t.Fatalf("Field = %q, want %q (error: %v)", ie.Field, field, err)
	}
	if ie.Reason == "" {
		t.Fatalf("Reason is empty (error: %v)", err)
	}
}

func goodSpec() Spec {
	return Spec{Queue: "thumbnails", Tenant: "tenant-42", Payload: []byte("x")}
}

func TestInvalidErrorFormatAndUnwrap(t *testing.T) {
	err := &InvalidError{Field: "queue", Reason: "must not be empty"}
	if got, want := err.Error(), "invalid queue: must not be empty"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Error("errors.Is(err, ErrInvalid) = false")
	}
}

func TestValidateName(t *testing.T) {
	ok := []string{"a", "Q1", "thumbnails", "tenant-42", "a.b_c-d", strings.Repeat("x", 128)}
	bad := []string{"", "a b", "q\t1", "-lead", ".x", "x\x00", strings.Repeat("x", 129), "ünï", "a/b"}
	for _, v := range ok {
		if err := ValidateName("queue", v); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
	for _, v := range bad {
		err := ValidateName("queue", v)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: got %v, want ErrInvalid", v, err)
		}
	}
}

func TestValidateNameReportsTheCallersField(t *testing.T) {
	wantInvalid(t, ValidateName("tenant", ""), "tenant")
	wantInvalid(t, ValidateName("queue", "a b"), "queue")
}

func TestValidateSpecAcceptsAGoodRequest(t *testing.T) {
	if err := ValidateSpec(goodSpec()); err != nil {
		t.Fatal(err)
	}
	full := Spec{
		Queue: "q", Tenant: "t", Payload: make([]byte, MaxPayloadBytes),
		Delay: MaxDelay, MaxAttempts: MaxMaxAttempts, IdempotencyKey: strings.Repeat("k", 255),
	}
	if err := ValidateSpec(full); err != nil {
		t.Fatalf("a request at every limit: %v", err)
	}
	scheduled := goodSpec()
	scheduled.RunAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := ValidateSpec(scheduled); err != nil {
		t.Fatalf("RunAt with Delay 0: %v", err)
	}
}

func TestValidateSpecHostileNames(t *testing.T) {
	for _, v := range []string{"", "a b", "q\t1", "-lead", "x\x00", strings.Repeat("x", 129), "ünï"} {
		s := goodSpec()
		s.Queue = v
		wantInvalid(t, ValidateSpec(s), "queue")
		s = goodSpec()
		s.Tenant = v
		wantInvalid(t, ValidateSpec(s), "tenant")
	}
}

func TestValidateSpecPayloadBoundary(t *testing.T) {
	for _, n := range []int{0, 262144} {
		s := goodSpec()
		s.Payload = make([]byte, n)
		if err := ValidateSpec(s); err != nil {
			t.Errorf("payload of %d bytes: %v", n, err)
		}
	}
	s := goodSpec()
	s.Payload = make([]byte, 262145)
	wantInvalid(t, ValidateSpec(s), "payload")
}

func TestValidateSpecDelay(t *testing.T) {
	s := goodSpec()
	s.Delay = -time.Microsecond
	wantInvalid(t, ValidateSpec(s), "delay")
	s.Delay = MaxDelay + time.Microsecond
	wantInvalid(t, ValidateSpec(s), "delay")
	s.Delay = time.Second + time.Nanosecond
	wantInvalid(t, ValidateSpec(s), "delay")
}

func TestValidateSpecDelayAndRunAtExclusive(t *testing.T) {
	s := goodSpec()
	s.Delay = time.Second
	s.RunAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wantInvalid(t, ValidateSpec(s), "run_at")
}

func TestValidateSpecRunAtRange(t *testing.T) {
	// The range is in UTC years, whatever zone the instant is written in.
	plus5 := time.FixedZone("plus5", 5*60*60)
	minus5 := time.FixedZone("minus5", -5*60*60)
	for name, at := range map[string]time.Time{
		"start of 2000":             time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		"2026":                      time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		"last nanosecond of 2999":   time.Date(2999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		"1999 locally, 2000 in UTC": time.Date(1999, 12, 31, 23, 0, 0, 0, minus5),
		"3000 locally, 2999 in UTC": time.Date(3000, 1, 1, 4, 0, 0, 0, plus5),
	} {
		s := goodSpec()
		s.RunAt = at
		if err := ValidateSpec(s); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, at := range map[string]time.Time{
		"1999":                      time.Date(1999, 6, 1, 0, 0, 0, 0, time.UTC),
		"last nanosecond of 1999":   time.Date(1999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		"start of 3000":             time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC),
		"year 1":                    time.Date(1, 1, 2, 0, 0, 0, 0, time.UTC),
		"the Unix epoch":            time.Unix(0, 1),
		"2000 locally, 1999 in UTC": time.Date(2000, 1, 1, 0, 30, 0, 0, plus5),
		"2999 locally, 3000 in UTC": time.Date(2999, 12, 31, 23, 0, 0, 0, minus5),
	} {
		t.Run(name, func(t *testing.T) {
			s := goodSpec()
			s.RunAt = at
			wantInvalid(t, ValidateSpec(s), "run_at")
		})
	}
}

func TestValidateSpecMaxAttempts(t *testing.T) {
	for _, n := range []int32{0, 1, 100} {
		s := goodSpec()
		s.MaxAttempts = n
		if err := ValidateSpec(s); err != nil {
			t.Errorf("MaxAttempts %d: %v", n, err)
		}
	}
	for _, n := range []int32{-1, 101} {
		s := goodSpec()
		s.MaxAttempts = n
		wantInvalid(t, ValidateSpec(s), "max_attempts")
	}
}

func TestValidateSpecIdempotencyKey(t *testing.T) {
	for _, k := range []string{"", strings.Repeat("k", 255), "héllo"} {
		s := goodSpec()
		s.IdempotencyKey = k
		if err := ValidateSpec(s); err != nil {
			t.Errorf("key %q: %v", k, err)
		}
	}
	for _, k := range []string{strings.Repeat("k", 256), "\xff", "a\x00b"} {
		s := goodSpec()
		s.IdempotencyKey = k
		wantInvalid(t, ValidateSpec(s), "idempotency_key")
	}
}

func TestValidateDurationsNeedWholeMicroseconds(t *testing.T) {
	wantInvalid(t, ValidateTTL(30*time.Second+time.Nanosecond), "ttl")
	wantInvalid(t, ValidateTTL(999*time.Millisecond), "ttl")
	if err := ValidateTTL(time.Hour); err != nil {
		t.Errorf("ValidateTTL(1h): %v", err)
	}
	wantInvalid(t, ValidateTTL(time.Hour+time.Microsecond), "ttl")
	wantInvalid(t, ValidateDelay("delay", -time.Microsecond), "delay")
	if err := ValidateDelay("delay", MaxDelay); err != nil {
		t.Errorf("ValidateDelay(MaxDelay): %v", err)
	}
}

func TestValidateTTLBounds(t *testing.T) {
	for _, d := range []time.Duration{MinTTL, MinTTL + time.Microsecond, 30 * time.Second, MaxTTL} {
		if err := ValidateTTL(d); err != nil {
			t.Errorf("ValidateTTL(%v): %v", d, err)
		}
	}
	for _, d := range []time.Duration{0, -time.Second, MinTTL - time.Microsecond, MaxTTL + time.Microsecond} {
		wantInvalid(t, ValidateTTL(d), "ttl")
	}
}

func TestValidateDelayBoundsAndField(t *testing.T) {
	for _, d := range []time.Duration{0, time.Microsecond, MaxDelay} {
		if err := ValidateDelay("retry_delay", d); err != nil {
			t.Errorf("ValidateDelay(%v): %v", d, err)
		}
	}
	for _, d := range []time.Duration{-time.Microsecond, MaxDelay + time.Microsecond, time.Nanosecond} {
		wantInvalid(t, ValidateDelay("retry_delay", d), "retry_delay")
	}
}

func TestValidateResultBoundary(t *testing.T) {
	for _, n := range []int{0, 65536} {
		if err := ValidateResult(make([]byte, n)); err != nil {
			t.Errorf("result of %d bytes: %v", n, err)
		}
	}
	wantInvalid(t, ValidateResult(make([]byte, 65537)), "result")
}

func TestValidateWorkerID(t *testing.T) {
	for _, v := range []string{"w-1", "!", "~", "host:42/slot_3", strings.Repeat("w", 128)} {
		if err := ValidateWorkerID(v); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
	for _, v := range []string{"", "a b", strings.Repeat("w", 129), "w\x7f", "w\x00", "wé", "w\n"} {
		wantInvalid(t, ValidateWorkerID(v), "worker_id")
	}
}

func TestValidateBatch(t *testing.T) {
	for _, n := range []int{1, 2, MaxBatch} {
		if err := ValidateBatch("items", n, MaxBatch); err != nil {
			t.Errorf("n=%d: %v", n, err)
		}
	}
	for _, n := range []int{0, -1, MaxBatch + 1} {
		wantInvalid(t, ValidateBatch("items", n, MaxBatch), "items")
	}
	if err := ValidateBatch("acks", MaxAckBatch, MaxAckBatch); err != nil {
		t.Errorf("a full ack batch: %v", err)
	}
	wantInvalid(t, ValidateBatch("acks", MaxAckBatch+1, MaxAckBatch), "acks")
}

func TestTruncateError(t *testing.T) {
	long := strings.Repeat("é", 3000) // 6000 bytes
	got := TruncateError(long)
	if len(got) > MaxErrorBytes || !utf8.ValidString(got) || len(got) < MaxErrorBytes-3 {
		t.Fatalf("len=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if TruncateError("a\x00b\xff") != "ab�" {
		t.Fatal("NUL must be removed and invalid bytes replaced")
	}
}

func TestTruncateErrorCutsOnARuneBoundary(t *testing.T) {
	// A rune that straddles byte 4096 must be dropped whole. The padding
	// shifts the runes so that the limit falls 0, 1, 2 or 3 bytes into one,
	// which makes the cut back up by up to 3 bytes (a 4-byte rune after one
	// byte of padding is the worst case).
	for _, r := range []string{"é", "€", "😀"} {
		for pad := 0; pad <= 3; pad++ {
			in := strings.Repeat("a", pad) + strings.Repeat(r, 3000)
			got := TruncateError(in)
			if !utf8.ValidString(got) {
				t.Errorf("%q after %d bytes of padding: result is not valid UTF-8", r, pad)
			}
			if len(got) > MaxErrorBytes || len(got) < MaxErrorBytes-3 {
				t.Errorf("%q after %d bytes of padding: len = %d, want in [%d, %d]", r, pad, len(got), MaxErrorBytes-3, MaxErrorBytes)
			}
			if !strings.HasPrefix(in, got) {
				t.Errorf("%q after %d bytes of padding: result is not a prefix of the input", r, pad)
			}
			// The longest prefix that ends on a rune boundary: no more is
			// cut than the straddling rune.
			if want := MaxErrorBytes - (MaxErrorBytes-pad)%len(r); len(got) != want {
				t.Errorf("%q after %d bytes of padding: len = %d, want %d", r, pad, len(got), want)
			}
		}
	}
	if got := TruncateError(strings.Repeat("a", 5000)); len(got) != MaxErrorBytes {
		t.Errorf("ASCII text: len = %d, want %d", len(got), MaxErrorBytes)
	}
}

func TestTruncateErrorLeavesShortValidTextAlone(t *testing.T) {
	for _, in := range []string{"", "boom", "héllo wörld", strings.Repeat("x", MaxErrorBytes)} {
		if got := TruncateError(in); got != in {
			t.Errorf("TruncateError(%.20q) changed the text", in)
		}
	}
}

var nameReference = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func FuzzValidateName(f *testing.F) {
	for _, s := range []string{"", "a", "Q1", "a b", "q\t1", "-lead", ".x", "x\x00", "ünï", "a/b", "tenant-42", "a.b_c-d", "x\n", "\xff", strings.Repeat("x", 128), strings.Repeat("x", 129)} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v string) {
		err := ValidateName("queue", v)
		if want := nameReference.MatchString(v); (err == nil) != want {
			t.Fatalf("ValidateName(%q) = %v, but the reference regexp says valid=%v", v, err, want)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Fatalf("ValidateName(%q) = %v, which does not wrap ErrInvalid", v, err)
		}
	})
}

func FuzzValidateSpec(f *testing.F) {
	// Arguments: queue, tenant, payload length, delay in nanoseconds, run_at
	// in Unix nanoseconds (0 = unset), max attempts, idempotency key.
	f.Add("q", "t", int32(0), int64(0), int64(0), int32(0), "")
	f.Add("q", "t", int32(MaxPayloadBytes), int64(MaxDelay), int64(0), int32(100), "k")
	f.Add("q", "t", int32(MaxPayloadBytes+1), int64(0), int64(0), int32(0), "")
	f.Add("q", "t", int32(1), int64(time.Second), int64(1), int32(0), "")
	f.Add("q", "t", int32(1), int64(0), int64(1), int32(101), "a\x00b")
	f.Add("", "a b", int32(-1), int64(-1), int64(0), int32(-1), "\xff")
	f.Add("q", "t", int32(1), int64(0), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), int32(0), "")
	f.Add("q", "t", int32(1), int64(0), time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC).UnixNano(), int32(0), "")
	f.Fuzz(func(t *testing.T, queue, tenant string, payloadLen int32, delay, runAtNanos int64, maxAttempts int32, key string) {
		s := Spec{
			Queue: queue, Tenant: tenant,
			Payload:        make([]byte, min(max(payloadLen, 0), MaxPayloadBytes+2)),
			Delay:          time.Duration(delay),
			MaxAttempts:    maxAttempts,
			IdempotencyKey: key,
		}
		if runAtNanos != 0 {
			s.RunAt = time.Unix(0, runAtNanos)
		}
		err := ValidateSpec(s)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error %v does not wrap ErrInvalid", err)
			}
			return
		}
		switch {
		case !nameReference.MatchString(s.Queue), !nameReference.MatchString(s.Tenant):
			t.Fatalf("accepted a bad name: queue %q tenant %q", s.Queue, s.Tenant)
		case len(s.Payload) > MaxPayloadBytes:
			t.Fatalf("accepted a payload of %d bytes", len(s.Payload))
		case s.MaxAttempts != 0 && (s.MaxAttempts < 1 || s.MaxAttempts > MaxMaxAttempts):
			t.Fatalf("accepted max_attempts %d", s.MaxAttempts)
		case s.Delay > 0 && !s.RunAt.IsZero():
			t.Fatalf("accepted Delay %v together with RunAt %v", s.Delay, s.RunAt)
		case !s.RunAt.IsZero() && (s.RunAt.UTC().Year() < 2000 || s.RunAt.UTC().Year() >= 3000):
			t.Fatalf("accepted RunAt %v", s.RunAt)
		case s.Delay < 0 || s.Delay > MaxDelay || s.Delay%time.Microsecond != 0:
			t.Fatalf("accepted Delay %v", s.Delay)
		case len(s.IdempotencyKey) > MaxIdempotencyKeyBytes || !utf8.ValidString(s.IdempotencyKey) || strings.ContainsRune(s.IdempotencyKey, 0):
			t.Fatalf("accepted idempotency key %q", s.IdempotencyKey)
		}
	})
}
