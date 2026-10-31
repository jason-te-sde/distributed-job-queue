package pgstore_test

import (
	"strings"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore"
)

func TestNotifyPayloadRoundTrip(t *testing.T) {
	for _, c := range []struct{ queue, tenant string }{
		{"q", "t"},
		{"emails", "acme-corp"},
		{"A.b_c-9", "0.tenant"},
		{strings.Repeat("a", job.MaxNameBytes), strings.Repeat("b", job.MaxNameBytes)},
	} {
		payload := pgstore.NotifyPayload(c.queue, c.tenant)
		if want := c.queue + "\t" + c.tenant; payload != want {
			t.Errorf("NotifyPayload(%q, %q) = %q, want %q", c.queue, c.tenant, payload, want)
		}
		queue, tenant, ok := pgstore.ParseNotifyPayload(payload)
		if !ok || queue != c.queue || tenant != c.tenant {
			t.Errorf("ParseNotifyPayload(%q) = %q, %q, %t; want %q, %q, true", payload, queue, tenant, ok, c.queue, c.tenant)
		}
	}
}

func TestParseNotifyPayloadRejects(t *testing.T) {
	for name, payload := range map[string]string{
		"no tab":                  "q",
		"two tabs":                "q\tt\tx",
		"empty queue":             "\tt",
		"empty tenant":            "q\t",
		"empty payload":           "",
		"only a tab":              "\t",
		"a space in the queue":    "a b\tt",
		"a space in the tenant":   "q\tt u",
		"a leading dash":          "-q\tt",
		"a NUL in the tenant":     "q\tt\x00",
		"a name over the limit":   strings.Repeat("a", job.MaxNameBytes+1) + "\tt",
		"non-ASCII in the queue":  "ünï\tt",
		"a newline in the tenant": "q\tt\n",
	} {
		if queue, tenant, ok := pgstore.ParseNotifyPayload(payload); ok {
			t.Errorf("%s: ParseNotifyPayload(%q) = %q, %q, true; want it rejected", name, payload, queue, tenant)
		}
	}
}

// A NOTIFY payload can come from anyone with access to the database, so the
// listener must not trust it: parsing never panics, and anything it accepts is
// exactly what NotifyPayload would have produced.
func FuzzParseNotifyPayload(f *testing.F) {
	for _, seed := range []string{"q\tt", "q", "q\tt\tx", "\tt", "", "a b\tt", "q\t\x00", "ünï\tt", "-a\tb"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload string) {
		queue, tenant, ok := pgstore.ParseNotifyPayload(payload)
		if !ok {
			return
		}
		if job.ValidateName("queue", queue) != nil || job.ValidateName("tenant", tenant) != nil {
			t.Fatalf("ParseNotifyPayload(%q) accepted the invalid names %q, %q", payload, queue, tenant)
		}
		if got := pgstore.NotifyPayload(queue, tenant); got != payload {
			t.Fatalf("NotifyPayload(%q, %q) = %q, want the input %q", queue, tenant, got, payload)
		}
	})
}
