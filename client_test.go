package jobqueue_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	jobqueue "github.com/jason-te-sde/distributed-job-queue"
	"github.com/jason-te-sde/distributed-job-queue/internal/broker"
)

// tokensFor returns the authenticator of a tokens file with one producer of
// the tenant, whose token is token.
func tokensFor(t *testing.T, tenant, token string) *broker.Authenticator {
	t.Helper()
	sum := sha256.Sum256([]byte(token))
	auth, err := broker.LoadTokens(strings.NewReader("producer tenant=" + tenant + " " + hex.EncodeToString(sum[:]) + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestDialUsesRoundRobin(t *testing.T) {
	c := newCluster(t, clusterOpts{brokers: 2})
	p := jobqueue.NewProducer(c.dial(), jobqueue.ProducerOptions{})
	ask := func() {
		t.Helper()
		// A missing job is a final answer, so each call is one request.
		if _, err := p.Get(t.Context(), 1); !errors.Is(err, jobqueue.ErrNotFound) {
			t.Fatalf("Get = %v, want ErrNotFound", err)
		}
	}

	// The connection to the second broker may still be coming up: ask until
	// both have answered, then count.
	deadline := time.Now().Add(5 * time.Second)
	for c.brokers[0].callCount("GetJob") == 0 || c.brokers[1].callCount("GetJob") == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a broker never served a call: %d and %d", c.brokers[0].callCount("GetJob"), c.brokers[1].callCount("GetJob"))
		}
		ask()
	}
	base := [2]int{c.brokers[0].callCount("GetJob"), c.brokers[1].callCount("GetJob")}
	for range 20 {
		ask()
	}
	for i, b := range c.brokers {
		if got := b.callCount("GetJob") - base[i]; got < 5 {
			t.Errorf("broker %d served %d of 20 calls, want at least 5: calls must be spread over both", i, got)
		}
	}
}

func TestWithTokenRequiresTransportChoice(t *testing.T) {
	_, err := jobqueue.Dial("localhost:7070", jobqueue.WithToken("secret"))
	if !errors.Is(err, jobqueue.ErrInvalid) {
		t.Fatalf("Dial with a token and no transport choice = %v, want ErrInvalid", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("the error %q repeats the token", err)
	}

	for name, opts := range map[string][]jobqueue.DialOption{
		"token and TLS":      {jobqueue.WithToken("secret"), jobqueue.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12})},
		"token and insecure": {jobqueue.WithToken("secret"), jobqueue.WithInsecure()},
		"insecure only":      {jobqueue.WithInsecure()},
		"nil TLS config":     {jobqueue.WithTLS(nil)},
	} {
		conn, err := jobqueue.Dial("localhost:7070", opts...)
		if err != nil {
			t.Errorf("%s: Dial = %v, want success", name, err)
			continue
		}
		_ = conn.Close()
	}

	for name, opts := range map[string][]jobqueue.DialOption{
		"TLS and insecure": {jobqueue.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}), jobqueue.WithInsecure()},
		"empty token":      {jobqueue.WithInsecure(), jobqueue.WithToken("")},
	} {
		if _, err := jobqueue.Dial("localhost:7070", opts...); !errors.Is(err, jobqueue.ErrInvalid) {
			t.Errorf("%s: Dial = %v, want ErrInvalid", name, err)
		}
	}
}

func TestDialSendsTheBearerToken(t *testing.T) {
	c := newCluster(t, clusterOpts{auth: tokensFor(t, "acme", "s3cret")})

	good := c.producer(jobqueue.WithToken("s3cret"))
	res, err := good.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.get(res.ID).Tenant; got != "acme" {
		t.Errorf("tenant = %q, want the token's tenant acme", got)
	}

	for name, p := range map[string]*jobqueue.Producer{
		"no token":    c.producer(),
		"wrong token": c.producer(jobqueue.WithToken("wrong")),
	} {
		before := c.brokers[0].callCount("Enqueue")
		_, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"})
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: Enqueue = %v, want Unauthenticated", name, err)
		}
		if got := c.brokers[0].callCount("Enqueue") - before; got != 1 {
			t.Errorf("%s: the broker saw %d attempts, want 1: Unauthenticated is not retried", name, got)
		}
	}
}

func TestDialOverTLSWithToken(t *testing.T) {
	// httptest owns a certificate for 127.0.0.1 and example.com; borrow it.
	cert := httptest.NewUnstartedServer(http.NotFoundHandler())
	cert.StartTLS()
	t.Cleanup(cert.Close)
	pool := x509.NewCertPool()
	pool.AddCert(cert.Certificate())
	serverCert := cert.TLS.Certificates[0]

	c := newCluster(t, clusterOpts{
		auth:  tokensFor(t, "acme", "s3cret"),
		creds: grpc.Creds(credentials.NewServerTLSFromCert(&serverCert)),
	})
	target, resolve := c.target()
	conn, err := jobqueue.Dial(target,
		jobqueue.WithTLS(&tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12}),
		jobqueue.WithToken("s3cret"),
		jobqueue.WithGRPCOptions(resolve))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	p := jobqueue.NewProducer(conn, jobqueue.ProducerOptions{})
	res, err := p.Enqueue(t.Context(), jobqueue.EnqueueParams{Queue: "q"})
	if err != nil {
		t.Fatalf("Enqueue over TLS = %v", err)
	}
	if got := c.get(res.ID).Tenant; got != "acme" {
		t.Errorf("tenant = %q, want acme", got)
	}

	// A plaintext client cannot talk to the TLS broker at all.
	plain := c.producer(jobqueue.WithToken("s3cret"))
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if _, err := plain.Get(ctx, res.ID); err == nil {
		t.Error("a plaintext client got an answer from a TLS broker")
	}
}
