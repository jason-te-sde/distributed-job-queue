package river

import (
	"strings"
	"testing"
	"time"

	"github.com/jason-te-sde/distributed-job-queue/bench/internal/exp"
)

func TestRunRefusesBadOptions(t *testing.T) {
	ok := Options{DatabaseURL: "postgres://127.0.0.1:1/none", Rate: 100, Duration: time.Second}
	for name, tc := range map[string]struct {
		mutate func(*Options)
		want   string
	}{
		"no database":          {func(o *Options) { o.DatabaseURL = "" }, "database URL"},
		"neither rate nor max": {func(o *Options) { o.Rate = 0 }, "--rate"},
		"both rate and max":    {func(o *Options) { o.FindMax = true }, "alternatives"},
		"payload under stamp":  {func(o *Options) { o.PayloadBytes = 4 }, "payload"},
		"negative concurrency": {func(o *Options) { o.Concurrency = -1 }, "concurrency"},
		"cooldown too short":   {func(o *Options) { o.FetchCooldown = time.Microsecond }, "fetch-cooldown"},
		"cooldown over a poll": {func(o *Options) { o.FetchCooldown = 2 * time.Second }, "fetch-cooldown"},
		"negative duration":    {func(o *Options) { o.Duration = -time.Second }, "duration"},
	} {
		o := ok
		tc.mutate(&o)
		err := Run(t.Context(), o, func(exp.Result) {})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to mention %q", name, err, tc.want)
		}
	}
}

func TestPayloadFloorIsTheStamp(t *testing.T) {
	validate := func(n int) error {
		return Options{DatabaseURL: "postgres://127.0.0.1:1/none", Rate: 100, Duration: time.Second, PayloadBytes: n}.withDefaults().validate()
	}
	if err := validate(exp.StampBytes - 1); err == nil || !strings.Contains(err.Error(), "payload") {
		t.Errorf("%d bytes, one under the stamp: error = %v, want it refused for the payload", exp.StampBytes-1, err)
	}
	if err := validate(exp.StampBytes); err != nil {
		t.Errorf("%d bytes, the size of the stamp, was refused: %v", exp.StampBytes, err)
	}
}

func TestParamsLabelTheRunAsLibraryMode(t *testing.T) {
	p := Options{Duration: time.Second, FetchCooldown: time.Millisecond}.withDefaults().params()
	if !strings.Contains(p["mode"], "no network hop") || !strings.Contains(p["mode"], "not a like-for-like") {
		t.Errorf("params[mode] = %q, want it to say that River is a library with no network hop and not a like-for-like comparison", p["mode"])
	}
	if p["fetch_cooldown"] != "1ms" {
		t.Errorf("params[fetch_cooldown] = %q, want the setting stated: it limits River's throughput", p["fetch_cooldown"])
	}
	if p["river_version"] == "" {
		t.Error("params[river_version] is empty")
	}
}
