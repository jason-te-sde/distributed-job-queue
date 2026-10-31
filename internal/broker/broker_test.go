package broker

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testConfig is a valid configuration that never reaches a database: tests of
// what New refuses before it connects use it.
func testConfig(t testing.TB) Config {
	t.Helper()
	cfg, err := ParseFlags([]string{
		"--grpc-addr=127.0.0.1:0", "--http-addr=127.0.0.1:0",
		"--database-url=postgres://127.0.0.1:1/never-reached?sslmode=disable",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func writeTokens(t testing.TB, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAuthIsNilOnlyWithoutATokensFile(t *testing.T) {
	// A nil Authenticator makes every caller an admin, so the only way to get
	// one is to configure no tokens file at all.
	a, err := loadAuth("")
	if a != nil || err != nil {
		t.Fatalf("loadAuth(\"\") = %v, %v; want nil, nil", a, err)
	}

	valid := writeTokens(t, "admin - "+hashOf("tok")+"\n")
	if a, err := loadAuth(valid); a == nil || err != nil {
		t.Fatalf("loadAuth(valid file) = %v, %v; want an Authenticator", a, err)
	}

	// A file with no tokens in it is a file: nobody authenticates, which is not
	// the same as everybody being an admin.
	empty := writeTokens(t, "# no tokens yet\n")
	a, err = loadAuth(empty)
	if a == nil || err != nil {
		t.Fatalf("loadAuth(empty file) = %v, %v; want an Authenticator that admits nobody", a, err)
	}
	if _, ok := a.Authenticate("tok"); ok {
		t.Error("an empty tokens file authenticated a token")
	}

	for name, path := range map[string]string{
		"missing":   filepath.Join(t.TempDir(), "absent"),
		"malformed": writeTokens(t, "admin - not-a-hash\n"),
		"directory": t.TempDir(),
	} {
		a, err := loadAuth(path)
		if a != nil || err == nil {
			t.Errorf("loadAuth(%s file) = %v, %v; want an error and no Authenticator: it must never fall back to nil", name, a, err)
		}
	}
}

func TestNewFailsWhenTheTokensFileCannotBeUsed(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, tc := range map[string]struct {
		file string
		want string
	}{
		"unreadable": {filepath.Join(t.TempDir(), "absent"), "tokens file"},
		"malformed":  {writeTokens(t, "admin - not-a-hash\n"), "tokens file line 1"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.TokensFile = tc.file
			b, err := New(t.Context(), cfg, log)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New = %v, %v; want an error mentioning %q", b, err, tc.want)
			}
			if b != nil {
				t.Error("New returned a broker together with an error")
			}
		})
	}
}

func TestNewValidatesTheConfiguration(t *testing.T) {
	cfg := testConfig(t)
	cfg.Scheduler = "lifo"
	if _, err := New(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil || !strings.Contains(err.Error(), "--scheduler") {
		t.Fatalf("New with a bad scheduler = %v, want the validation error", err)
	}
}

func TestKeepaliveEnforcementAdmitsTheSDKPing(t *testing.T) {
	// gRPC would otherwise apply its default, a ping every 5 minutes and no
	// pings on an idle connection, and close the connection of a worker whose
	// SDK pings every 30s. A ping every 30s must be later than MinTime, and
	// an idle connection must be allowed to send it.
	p := keepaliveEnforcement()
	if p.MinTime <= 0 || p.MinTime > sdkPingEvery {
		t.Errorf("MinTime = %v, want above 0 and at most the SDK's ping interval of %v", p.MinTime, sdkPingEvery)
	}
	if !p.PermitWithoutStream {
		t.Error("PermitWithoutStream is false: an idle connection of a worker between long polls could not ping")
	}
	if p.MinTime >= 5*time.Minute {
		t.Errorf("MinTime = %v is gRPC's default or worse", p.MinTime)
	}
}
