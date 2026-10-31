package jobqueue

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
)

// dialConfig is what the DialOptions collect for Dial.
type dialConfig struct {
	token    string
	hasToken bool

	tlsConfig *tls.Config
	hasTLS    bool
	insecure  bool

	grpcOptions []grpc.DialOption
}

// DialOption configures Dial.
type DialOption func(*dialConfig)

// WithToken authenticates every call with the bearer token, which the broker
// looks up in its tokens file. A token is a credential, so Dial refuses it
// unless the caller also says how it travels: WithTLS, or WithInsecure to
// send it in the clear, as a test against a local broker does. An empty token
// is refused as well, so that a variable that was never set cannot turn into
// an unauthenticated client unnoticed.
func WithToken(token string) DialOption {
	return func(c *dialConfig) { c.token, c.hasToken = token, true }
}

// WithTLS makes the connection use TLS with cfg, which holds the root CAs and
// the server name to verify, or a client certificate. A nil cfg means the
// defaults: the system's root CAs and the host of the target as the server
// name.
func WithTLS(cfg *tls.Config) DialOption {
	return func(c *dialConfig) { c.tlsConfig, c.hasTLS = cfg, true }
}

// WithInsecure makes the connection plaintext. Use it only for a broker on a
// loopback address or on a network that is trusted completely: nothing on the
// connection, a token included, is protected.
func WithInsecure() DialOption {
	return func(c *dialConfig) { c.insecure = true }
}

// WithGRPCOptions adds options of the gRPC library to the connection, for
// example an interceptor or a resolver. They come after the ones Dial sets
// itself, so they win where both name the same thing.
func WithGRPCOptions(opts ...grpc.DialOption) DialOption {
	return func(c *dialConfig) { c.grpcOptions = append(c.grpcOptions, opts...) }
}

// The defaults of RetryPolicy and ProducerOptions.
const (
	defaultRetryAttempts  = 5
	defaultRetryBase      = 50 * time.Millisecond
	defaultRetryCap       = 2 * time.Second
	defaultPerCallTimeout = 10 * time.Second
)

// RetryPolicy says how a Producer repeats a call that failed with a retryable
// error (see IsRetryable). The wait before attempt n+1 is drawn uniformly
// from [0, min(Cap, Base*2^(n-1))], so producers that fail together do not
// retry together.
type RetryPolicy struct {
	// Attempts is the most times a call is made, the first included: 1 means
	// no retry. A value below 1 means the default, 5.
	Attempts int
	// Base is the ceiling of the wait after the first failure. A value
	// below 1ns means the default, 50ms.
	Base time.Duration
	// Cap is the largest ceiling of the wait. A value below Base means Base;
	// a value of 0 means the default, 2s.
	Cap time.Duration
}

// withDefaults returns the policy with its zero and out-of-range fields
// replaced.
func (r RetryPolicy) withDefaults() RetryPolicy {
	if r.Attempts < 1 {
		r.Attempts = defaultRetryAttempts
	}
	if r.Base < 1 {
		r.Base = defaultRetryBase
	}
	if r.Cap == 0 {
		r.Cap = defaultRetryCap
	}
	r.Cap = max(r.Cap, r.Base)
	return r
}

// ProducerOptions configures a Producer.
type ProducerOptions struct {
	// Tenant is the tenant of every job whose EnqueueParams name none. Empty
	// leaves the choice to the broker: the tenant of the caller's token, or
	// "default" for an administrator.
	Tenant string
	// Retry says how failed calls are repeated.
	Retry RetryPolicy
	// PerCallTimeout bounds each attempt of a call, not the call as a whole;
	// the context passed to the call bounds that. A value of 0 or below means
	// the default, 10s.
	PerCallTimeout time.Duration
}
