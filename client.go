package jobqueue

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// maxMessageBytes is the largest gRPC message in either direction: 64
	// jobs of 256 KiB with room for the rest of the message. The broker
	// uses the same limit.
	maxMessageBytes = 20 << 20

	// serviceConfig spreads calls over every address the target resolves to.
	// The default, pick_first, would send them all to one broker and leave
	// the others idle.
	serviceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
)

// bearer sends a token in the authorization header of every call.
type bearer struct {
	token  string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

// RequireTransportSecurity is true when the connection uses TLS, which makes
// the gRPC library refuse to send the token over anything else. A connection
// that the caller chose to make plaintext carries the token in the clear.
func (b bearer) RequireTransportSecurity() bool { return b.secure }

// Dial opens a client connection to the broker or brokers that target names,
// for NewProducer and NewWorker. The connection is lazy, as grpc.NewClient's
// is: the first call connects it.
//
// A target of the form "dns:///jobqueue.<namespace>.svc:7070" resolves to every
// address the name has, usually one per broker, and calls are spread over them
// round-robin. If a broker fails or is drained, the next call goes to another.
// The connection allows messages of 20 MiB in both directions, which holds a
// batch of 64 payloads at the size limit, and sends a keepalive ping after 30s
// without traffic while a call is in flight, so that a connection cut silently
// is noticed.
//
// The caller chooses how the connection is secured, with WithTLS or
// WithInsecure, and may authenticate with WithToken. Dial closes nothing:
// the caller closes the connection. Errors wrap ErrInvalid when the options
// contradict each other.
func Dial(target string, opts ...DialOption) (*grpc.ClientConn, error) {
	var cfg dialConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	dopts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout}),
	}
	switch {
	case cfg.hasTLS:
		dopts = append(dopts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.tlsConfig)))
	case cfg.insecure:
		dopts = append(dopts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if cfg.hasToken {
		dopts = append(dopts, grpc.WithPerRPCCredentials(bearer{token: cfg.token, secure: cfg.hasTLS}))
	}
	dopts = append(dopts, cfg.grpcOptions...)

	conn, err := grpc.NewClient(target, dopts...)
	if err != nil {
		return nil, fmt.Errorf("jobqueue: dial %q: %w", target, err)
	}
	return conn, nil
}

// validate checks that the options agree with each other.
func (c *dialConfig) validate() error {
	switch {
	case c.hasTLS && c.insecure:
		return fmt.Errorf("%w: WithTLS and WithInsecure contradict each other", ErrInvalid)
	case c.hasToken && c.token == "":
		return fmt.Errorf("%w: WithToken was given an empty token", ErrInvalid)
	case c.hasToken && !c.hasTLS && !c.insecure:
		return fmt.Errorf("%w: WithToken needs WithTLS, or WithInsecure to send the token in the clear", ErrInvalid)
	}
	return nil
}
