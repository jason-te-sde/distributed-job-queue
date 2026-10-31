package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// env returns a getenv that reads from m.
func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// wantError fails unless err is non-nil and mentions every part.
func wantError(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want one that mentions %q", parts)
	}
	for _, p := range parts {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("error %q does not mention %q", err, p)
		}
	}
}

// testCert writes a self-signed certificate for 127.0.0.1 and its key to a
// temporary directory and returns the two paths. The certificate is its own
// CA, so the same file serves as the server's certificate and as --tls-ca.
func testCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "thumbnail test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestParseWorkTakesTheDatabaseURLFromTheEnvironment(t *testing.T) {
	cfg, err := parseWork([]string{"--addr", "b:1", "--insecure"}, env(map[string]string{"JOBQUEUE_DATABASE_URL": "postgres://from-env"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.databaseURL != "postgres://from-env" {
		t.Errorf("database URL = %q, want the one from the environment", cfg.databaseURL)
	}

	cfg, err = parseWork([]string{"--addr", "b:1", "--insecure", "--database-url", "postgres://from-flag"}, env(map[string]string{"JOBQUEUE_DATABASE_URL": "postgres://from-env"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.databaseURL != "postgres://from-flag" {
		t.Errorf("database URL = %q, want the flag to win over the environment", cfg.databaseURL)
	}
}

func TestParseWorkNeedsADatabaseURL(t *testing.T) {
	_, err := parseWork([]string{"--addr", "b:1", "--insecure"}, env(nil))
	wantError(t, err, "--database-url", "JOBQUEUE_DATABASE_URL")
}

func TestParseWorkDefaultsAndBounds(t *testing.T) {
	cfg, err := parseWork([]string{"--addr", "b:1", "--insecure", "--database-url", "u"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.concurrency != 4 {
		t.Errorf("default concurrency = %d, want 4", cfg.concurrency)
	}
	_, err = parseWork([]string{"--addr", "b:1", "--insecure", "--database-url", "u", "--concurrency", "0"}, env(nil))
	wantError(t, err, "--concurrency")
}

func TestParseNeedsAnAddress(t *testing.T) {
	_, err := parseWork([]string{"--insecure", "--database-url", "u"}, env(nil))
	wantError(t, err, "--addr")
	_, err = parseProduce([]string{"--insecure"}, env(nil))
	wantError(t, err, "--addr")
}

func TestParseProduceDefaultsAndBounds(t *testing.T) {
	cfg, err := parseProduce([]string{"--addr", "b:1", "--insecure"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.count != 10 || cfg.tenant != "" {
		t.Errorf("defaults = count %d tenant %q, want 10 and no tenant", cfg.count, cfg.tenant)
	}
	cfg, err = parseProduce([]string{"--addr", "b:1", "--insecure", "--count", "3", "--tenant", "acme"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.count != 3 || cfg.tenant != "acme" {
		t.Errorf("got count %d tenant %q, want 3 and acme", cfg.count, cfg.tenant)
	}
	_, err = parseProduce([]string{"--addr", "b:1", "--insecure", "--count", "0"}, env(nil))
	wantError(t, err, "--count")
}

func TestParseRefusesToGuessTheTransport(t *testing.T) {
	cert, _ := testCert(t)
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want []string
	}{
		{"nothing", nil, nil, []string{"--tls-ca", "--insecure"}},
		{"a token alone", []string{"--token", "t"}, nil, []string{"--tls-ca", "--insecure"}},
		{"a token from the environment alone", nil, map[string]string{"JOBQUEUE_TOKEN": "t"}, []string{"--tls-ca", "--insecure"}},
		{"both", []string{"--insecure", "--tls-ca", cert}, nil, []string{"--tls-ca", "--insecure", "contradict"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseWork(append([]string{"--addr", "b:1", "--database-url", "u"}, tc.args...), env(tc.env))
			wantError(t, err, tc.want...)
			_, err = parseProduce(append([]string{"--addr", "b:1"}, tc.args...), env(tc.env))
			wantError(t, err, tc.want...)
		})
	}
}

func TestParseTokenComesFromTheFlagOrTheEnvironment(t *testing.T) {
	base := []string{"--addr", "b:1", "--insecure"}
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"none", nil, nil, ""},
		{"environment", nil, map[string]string{"JOBQUEUE_TOKEN": "from-env"}, "from-env"},
		{"flag", []string{"--token", "from-flag"}, nil, "from-flag"},
		{"flag over environment", []string{"--token", "from-flag"}, map[string]string{"JOBQUEUE_TOKEN": "from-env"}, "from-flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseProduce(append(append([]string{}, base...), tc.args...), env(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.transport.token != tc.want {
				t.Errorf("token = %q, want %q", cfg.transport.token, tc.want)
			}
		})
	}
}

func TestParseRefusesAnEmptyTokenFlag(t *testing.T) {
	// A variable that was never set must not turn into an unauthenticated
	// client unnoticed: --token "$UNSET" is an error, not a missing token.
	_, err := parseProduce([]string{"--addr", "b:1", "--insecure", "--token", ""}, env(map[string]string{"JOBQUEUE_TOKEN": "from-env"}))
	wantError(t, err, "--token")
}

func TestParseReportsHelpAndUnknownFlags(t *testing.T) {
	_, err := parseWork([]string{"-h"}, env(nil))
	if _, ok := errors.AsType[*helpError](err); !ok {
		t.Errorf("-h gave %v, want a help request", err)
	}
	_, err = parseWork([]string{"--no-such-flag"}, env(nil))
	wantError(t, err, "no-such-flag")
	if _, ok := errors.AsType[*helpError](err); ok {
		t.Error("an unknown flag was taken for a help request")
	}
}

func TestDialOptionsReadTheCAFile(t *testing.T) {
	cert, _ := testCert(t)
	opts, err := transport{caFile: cert}.dialOptions()
	if err != nil || len(opts) != 1 {
		t.Fatalf("a valid CA file gave %d options, err %v", len(opts), err)
	}

	_, err = transport{caFile: filepath.Join(t.TempDir(), "missing.pem")}.dialOptions()
	wantError(t, err, "--tls-ca", "missing.pem")

	notPEM := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = transport{caFile: notPEM}.dialOptions()
	wantError(t, err, "--tls-ca", "no certificate")
}

func TestDialOptionsForPlaintextAndToken(t *testing.T) {
	opts, err := transport{insecure: true}.dialOptions()
	if err != nil || len(opts) != 1 {
		t.Fatalf("--insecure gave %d options, err %v", len(opts), err)
	}
	opts, err = transport{insecure: true, token: "t"}.dialOptions()
	if err != nil || len(opts) != 2 {
		t.Fatalf("--insecure with a token gave %d options, err %v", len(opts), err)
	}
}

func TestRunRejectsWhatItCannotParse(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want int
		out  string // text that must be on stderr, or on stdout for a status of 0
	}{
		{"no command", nil, 2, "usage: thumbnail"},
		{"unknown command", []string{"frobnicate"}, 2, "frobnicate"},
		{"help", []string{"--help"}, 0, "usage: thumbnail"},
		{"command help", []string{"work", "-h"}, 0, "--database-url"},
		{"work without a transport", []string{"work", "--addr", "b:1", "--database-url", "postgres://127.0.0.1:1/none"}, 2, "--tls-ca"},
		{"produce without a transport", []string{"produce", "--addr", "b:1"}, 2, "--tls-ca"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			got := run(context.Background(), tc.args, env(nil), &stdout, &stderr)
			if got != tc.want {
				t.Errorf("status = %d, want %d (stderr %q)", got, tc.want, stderr.String())
			}
			text := stderr.String()
			if tc.want == 0 {
				text = stdout.String()
				if stderr.Len() != 0 {
					t.Errorf("stderr = %q, want empty", stderr.String())
				}
			}
			if !strings.Contains(text, tc.out) {
				t.Errorf("output %q does not contain %q", text, tc.out)
			}
		})
	}
}

func TestProcessFailsAnUnrenderablePayloadWithoutTouchingTheSink(t *testing.T) {
	// A nil sink panics if it is used, so this also shows the sink is not.
	for name, payload := range map[string][]byte{
		"not JSON":       []byte("{"),
		"size too big":   []byte(`{"seed":1,"size":4096}`),
		"size too small": []byte(`{"seed":1,"size":10}`),
		"empty payload":  nil,
	} {
		t.Run(name, func(t *testing.T) {
			wantError(t, process(context.Background(), nil, 1, 1, payload), "payload")
		})
	}
}

func TestHelpListsEveryFlagWithTwoDashesAndNoTrailingSpace(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if status := run(context.Background(), []string{"work", "-h"}, env(nil), &stdout, &stderr); status != 0 {
		t.Fatalf("status = %d: %s", status, stderr.String())
	}
	for _, flagName := range []string{"addr", "concurrency", "database-url", "insecure", "tls-ca", "token"} {
		if !strings.Contains(stdout.String(), "\n  --"+flagName) {
			t.Errorf("help does not list --%s:\n%s", flagName, stdout.String())
		}
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if strings.TrimRight(line, " ") != line {
			t.Errorf("help line %q ends with a space", line)
		}
	}
}
