package broker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// Role is what a caller may do. The zero Role is deliberately not one of
// them, so that a context that never went through UnaryAuth has no rights.
type Role uint8

// The roles of a token.
const (
	// RoleProducer enqueues jobs for one tenant and reads and cancels that
	// tenant's jobs.
	RoleProducer Role = iota + 1
	// RoleWorker leases jobs from its queues and reports on them.
	RoleWorker
	// RoleAdmin may call every method.
	RoleAdmin
)

// String returns the lower-case name of the role, or "role(N)" for a value
// that is not one.
func (r Role) String() string {
	switch r {
	case RoleProducer:
		return "producer"
	case RoleWorker:
		return "worker"
	case RoleAdmin:
		return "admin"
	}
	return "role(" + strconv.Itoa(int(r)) + ")"
}

// Principal is the caller that a token names.
type Principal struct {
	// Role says what the caller may do.
	Role Role
	// Tenant is the tenant a producer acts for; empty for the other roles.
	Tenant string
	// Queues are the queues a worker may use, sorted and without
	// duplicates; nil for the other roles.
	Queues []string
}

// Authenticator maps the SHA-256 of a token to the principal it names. The
// tokens themselves are never held: a leaked tokens file or memory dump does
// not reveal a credential.
type Authenticator struct {
	tokens map[[32]byte]Principal
}

// LoadTokens reads a tokens file. Each line is one of
//
//	producer tenant=<name> <sha256hex>
//	worker queues=<queue>[,<queue>...] <sha256hex>
//	admin - <sha256hex>
//
// where the hash is 64 lowercase hex characters, the SHA-256 of the token.
// Everything from a '#' to the end of a line is a comment, and blank lines are
// ignored. Names must satisfy job.ValidateName. A queue named twice in one
// line counts once. A malformed line, or a hash that appears twice, fails the
// whole file with an error of the form "tokens file line <n>: <reason>"; the
// error never contains a hash.
func LoadTokens(r io.Reader) (*Authenticator, error) {
	a := &Authenticator{tokens: map[[32]byte]Principal{}}
	seen := map[[32]byte]int{} // hash -> the line that first named it
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line, _, _ := strings.Cut(sc.Text(), "#")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		hash, p, err := parseTokenLine(fields)
		if err != nil {
			return nil, fmt.Errorf("tokens file line %d: %w", n, err)
		}
		if first, dup := seen[hash]; dup {
			return nil, fmt.Errorf("tokens file line %d: duplicate token hash, already on line %d", n, first)
		}
		seen[hash] = n
		a.tokens[hash] = p
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("tokens file line %d: %w", n+1, err)
	}
	return a, nil
}

// parseTokenLine reads the three fields of a tokens file line.
func parseTokenLine(fields []string) ([32]byte, Principal, error) {
	var hash [32]byte
	if len(fields) != 3 {
		return hash, Principal{}, fmt.Errorf("want 3 fields (<role> <binding> <sha256hex>), got %d", len(fields))
	}
	role, binding, hexHash := fields[0], fields[1], fields[2]

	var p Principal
	switch role {
	case "producer":
		tenant, ok := strings.CutPrefix(binding, "tenant=")
		if !ok {
			return hash, Principal{}, errors.New("a producer is bound to a tenant: write tenant=<name>")
		}
		if err := job.ValidateName("tenant", tenant); err != nil {
			return hash, Principal{}, err
		}
		p = Principal{Role: RoleProducer, Tenant: tenant}
	case "worker":
		list, ok := strings.CutPrefix(binding, "queues=")
		if !ok {
			return hash, Principal{}, errors.New("a worker is bound to queues: write queues=<queue>[,<queue>...]")
		}
		queues := strings.Split(list, ",")
		for _, q := range queues {
			if err := job.ValidateName("queue", q); err != nil {
				return hash, Principal{}, err
			}
		}
		slices.Sort(queues)
		p = Principal{Role: RoleWorker, Queues: slices.Compact(queues)}
	case "admin":
		if binding != "-" {
			return hash, Principal{}, errors.New("an admin has no binding: write - in its place")
		}
		p = Principal{Role: RoleAdmin}
	default:
		return hash, Principal{}, fmt.Errorf("unknown role %q: want producer, worker or admin", role)
	}

	if len(hexHash) != 2*len(hash) || strings.ToLower(hexHash) != hexHash {
		return hash, Principal{}, errors.New("the token hash must be 64 lowercase hex characters")
	}
	if _, err := hex.Decode(hash[:], []byte(hexHash)); err != nil {
		return hash, Principal{}, errors.New("the token hash must be 64 lowercase hex characters")
	}
	return hash, p, nil
}

// Authenticate returns the principal whose token hashes like token. An empty
// token never authenticates. The returned Principal owns its Queues slice.
// A nil Authenticator authenticates nobody.
func (a *Authenticator) Authenticate(token string) (Principal, bool) {
	if a == nil || token == "" {
		return Principal{}, false
	}
	p, ok := a.tokens[sha256.Sum256([]byte(token))]
	if !ok {
		return Principal{}, false
	}
	p.Queues = slices.Clone(p.Queues)
	return p, true
}

type principalKey struct{}

// UnaryAuth returns the interceptor that identifies the caller of every unary
// call and stores the Principal in the call's context, where PrincipalFrom
// finds it.
//
// With a nil Authenticator, which is possible only on a loopback address or
// under --insecure, every caller is an admin. Otherwise the call must carry
// exactly one "authorization: Bearer <token>" header naming a known token, or
// it fails with codes.Unauthenticated before any handler runs.
func UnaryAuth(a *Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if a == nil {
			return handler(context.WithValue(ctx, principalKey{}, Principal{Role: RoleAdmin}), req)
		}
		token, err := bearerToken(ctx)
		if err != nil {
			return nil, err
		}
		p, ok := a.Authenticate(token)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "unknown token")
		}
		return handler(context.WithValue(ctx, principalKey{}, p), req)
	}
}

// bearerToken extracts the token of the call's single Authorization header.
// The scheme is matched without regard to case, as RFC 9110 says; the token is
// taken exactly.
func bearerToken(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	switch len(values) {
	case 0:
		return "", status.Error(codes.Unauthenticated, "missing authorization header: send 'Bearer <token>'")
	case 1:
	default:
		return "", status.Error(codes.Unauthenticated, "more than one authorization header")
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", status.Error(codes.Unauthenticated, "authorization must be 'Bearer <token>'")
	}
	return token, nil
}

// PrincipalFrom returns the caller that UnaryAuth identified. For a context
// that never went through UnaryAuth it returns the zero Principal, whose role
// Authorize turns away from every method.
func PrincipalFrom(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey{}).(Principal)
	return p
}

// The method names that Authorize knows: the last element of the gRPC method.
const (
	methodEnqueue      = "Enqueue"
	methodLease        = "Lease"
	methodHeartbeat    = "Heartbeat"
	methodAck          = "Ack"
	methodRelease      = "Release"
	methodGetJob       = "GetJob"
	methodCancelJob    = "CancelJob"
	methodGetStats     = "GetStats"
	methodListDeadJobs = "ListDeadJobs"
	methodRequeueJob   = "RequeueJob"
	methodPurgeJobs    = "PurgeJobs"
)

// methodRoles says which roles may call which method. A method that is not in
// the table may be called by nobody.
var methodRoles = map[string][]Role{
	methodEnqueue:      {RoleProducer, RoleAdmin},
	methodLease:        {RoleWorker, RoleAdmin},
	methodHeartbeat:    {RoleWorker, RoleAdmin},
	methodAck:          {RoleWorker, RoleAdmin},
	methodRelease:      {RoleWorker, RoleAdmin},
	methodGetJob:       {RoleProducer, RoleAdmin},
	methodCancelJob:    {RoleProducer, RoleAdmin},
	methodGetStats:     {RoleAdmin},
	methodListDeadJobs: {RoleAdmin},
	methodRequeueJob:   {RoleAdmin},
	methodPurgeJobs:    {RoleAdmin},
}

// Authorize says whether p's role may call method, a bare method name such as
// "Enqueue". It returns nil, or a codes.PermissionDenied error that names the
// role and the method. It checks the role only: the tenant of a producer and
// the queues of a worker are checked by the handlers, which see the request.
func Authorize(p Principal, method string) error {
	if slices.Contains(methodRoles[method], p.Role) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "role %s may not call %s", p.Role, method)
}
