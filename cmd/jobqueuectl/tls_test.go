package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// selfSigned returns a certificate for 127.0.0.1 that signs itself, and its
// PEM. The same PEM is the CA file of a client that is to trust the server.
func selfSigned(t *testing.T, name string) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
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
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// writeFile writes content to a new file of the test's directory and returns
// its path.
func writeFile(t *testing.T, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCtlTLS(t *testing.T) {
	cert, certPEM := selfSigned(t, "the broker")
	_, otherPEM := selfSigned(t, "another CA")
	b := startBroker(t, nil, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12,
	})))
	if _, err := b.mem.Enqueue(t.Context(), []job.Spec{{Queue: "q", Tenant: "acme", Payload: []byte("x")}}); err != nil {
		t.Fatal(err)
	}

	goodCA := writeFile(t, "ca.pem", certPEM)
	wrongCA := writeFile(t, "other.pem", otherPEM)
	notPEM := writeFile(t, "ca.txt", []byte("this is not a certificate\n"))

	for name, tc := range map[string]struct {
		args []string
		code int
		want string // in stdout on success, in stderr otherwise
	}{
		"a good CA before the command":                   {[]string{"--tls-ca", goodCA, "--addr", b.addr, "stats", "--queue", "q"}, 0, "acme"},
		"a good CA after the command":                    {[]string{"stats", "--queue", "q", "--addr", b.addr, "--tls-ca", goodCA}, 0, "acme"},
		"no CA: the system roots do not know the server": {[]string{"--addr", b.addr, "stats", "--queue", "q"}, 1, "unknown authority"},
		"a CA that did not sign the server":              {[]string{"--tls-ca", wrongCA, "--addr", b.addr, "stats", "--queue", "q"}, 1, "unknown authority"},
		"a CA file without a certificate":                {[]string{"--tls-ca", notPEM, "--addr", b.addr, "stats", "--queue", "q"}, 1, "no PEM certificate"},
		"a CA file that is not there":                    {[]string{"--tls-ca", filepath.Join(t.TempDir(), "absent.pem"), "--addr", b.addr, "stats", "--queue", "q"}, 1, "no such file"},
		"--insecure against a TLS server":                {[]string{"--insecure", "--addr", b.addr, "stats", "--queue", "q"}, 1, "jobqueuectl: "},
	} {
		t.Run(name, func(t *testing.T) {
			r := runCtl(nil, append([]string{"--timeout", "5s"}, tc.args...)...)
			out := r.stderr
			if tc.code == 0 {
				out = r.stdout
			}
			if r.code != tc.code || !strings.Contains(out, tc.want) {
				t.Fatalf("jobqueuectl %v = exit %d, stdout %q, stderr %q; want exit %d with %q", tc.args, r.code, r.stdout, r.stderr, tc.code, tc.want)
			}
			if tc.code != 0 && r.stdout != "" {
				t.Fatalf("a failed command printed %q on stdout", r.stdout)
			}
		})
	}
}
