package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
)

// hashOf returns the line form of a token: the lower-case hex SHA-256.
func hashOf(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func TestRoleString(t *testing.T) {
	for r, want := range map[Role]string{RoleProducer: "producer", RoleWorker: "worker", RoleAdmin: "admin", 0: "role(0)", 9: "role(9)"} {
		if got := r.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", uint8(r), got, want)
		}
	}
}

func TestLoadTokensParsesRoles(t *testing.T) {
	file := strings.Join([]string{
		"# tokens for the staging cluster",
		"",
		"producer tenant=acme " + hashOf("p-acme") + "   # the billing service",
		"worker queues=emails " + hashOf("w-emails"),
		"worker queues=reports,emails,reports " + hashOf("w-multi") + "\r",
		"   admin   -   " + hashOf("a-root") + "  ",
		"\t",
		"producer tenant=Tenant_2.x-y " + hashOf("p-two"),
	}, "\n")
	a, err := LoadTokens(strings.NewReader(file))
	if err != nil {
		t.Fatalf("LoadTokens: %v", err)
	}
	tests := []struct {
		token string
		want  Principal
	}{
		{"p-acme", Principal{Role: RoleProducer, Tenant: "acme"}},
		{"p-two", Principal{Role: RoleProducer, Tenant: "Tenant_2.x-y"}},
		{"w-emails", Principal{Role: RoleWorker, Queues: []string{"emails"}}},
		{"w-multi", Principal{Role: RoleWorker, Queues: []string{"emails", "reports"}}},
		{"a-root", Principal{Role: RoleAdmin}},
	}
	for _, tc := range tests {
		got, ok := a.Authenticate(tc.token)
		if !ok {
			t.Errorf("Authenticate(%q) = false", tc.token)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Authenticate(%q) = %+v, want %+v", tc.token, got, tc.want)
		}
	}
	for _, unknown := range []string{"", "p-acme ", "P-ACME", hashOf("p-acme"), "nope"} {
		if p, ok := a.Authenticate(unknown); ok {
			t.Errorf("Authenticate(%q) = %+v, true; want false", unknown, p)
		}
	}
}

func TestAuthenticateHandsOutACopyOfQueues(t *testing.T) {
	a, err := LoadTokens(strings.NewReader("worker queues=a,b " + hashOf("w") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	first, _ := a.Authenticate("w")
	first.Queues[0] = "hijacked"
	second, _ := a.Authenticate("w")
	if !slices.Equal(second.Queues, []string{"a", "b"}) {
		t.Fatalf("a caller's write reached the stored principal: %v", second.Queues)
	}
}

// A tokens file that lists the hash of the empty string would otherwise make
// "Authorization: Bearer " (with nothing after the space) a working
// credential.
func TestAuthenticateRefusesTheEmptyToken(t *testing.T) {
	a, err := LoadTokens(strings.NewReader("admin - " + hashOf("") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := a.Authenticate(""); ok {
		t.Fatalf("the empty token authenticated as %+v", p)
	}
}

func TestAuthenticateNilAuthenticatorDeniesEverything(t *testing.T) {
	var a *Authenticator
	if p, ok := a.Authenticate("anything"); ok {
		t.Fatalf("a nil Authenticator accepted a token: %+v", p)
	}
}

func TestLoadTokensEmptyFile(t *testing.T) {
	a, err := LoadTokens(strings.NewReader("# nothing here\n\n"))
	if err != nil {
		t.Fatalf("LoadTokens: %v", err)
	}
	if _, ok := a.Authenticate("x"); ok {
		t.Fatal("an empty token file authenticated a token")
	}
}

func TestLoadTokensRejectsMalformed(t *testing.T) {
	good := hashOf("good")
	other := hashOf("other")
	tests := []struct {
		name     string
		file     string
		wantLine int
		wantText string // a fragment of the reason
	}{
		{"unknown role", "operator tenant=a " + good, 1, "role"},
		{"role in capitals", "Producer tenant=a " + good, 1, "role"},
		{"short hex", "producer tenant=a " + good[:63], 1, "64"},
		{"long hex", "producer tenant=a " + good + "0", 1, "64"},
		{"upper-case hex", "producer tenant=a " + strings.ToUpper(good), 1, "lowercase"},
		{"non-hex character", "producer tenant=a " + good[:63] + "g", 1, "lowercase"},
		{"no binding on a producer", "producer " + good, 1, "fields"},
		{"no binding on a worker", "worker " + good, 1, "fields"},
		{"empty tenant", "producer tenant= " + good, 1, "tenant"},
		{"producer with a dash", "producer - " + good, 1, "tenant="},
		{"producer bound to queues", "producer queues=a " + good, 1, "tenant="},
		{"worker bound to a tenant", "worker tenant=a " + good, 1, "queues="},
		{"worker with a dash", "worker - " + good, 1, "queues="},
		{"admin with a tenant", "admin tenant=a " + good, 1, "admin"},
		{"admin without the dash", "admin all " + good, 1, "admin"},
		{"bad queue name", "worker queues=a,-b " + good, 1, "queue"},
		{"empty queue in the list", "worker queues=a,,b " + good, 1, "queue"},
		{"empty queue list", "worker queues= " + good, 1, "queue"},
		{"queue with a space inside a list", "worker queues=a b " + good, 1, "fields"},
		{"queue with non-ASCII", "worker queues=ünï " + good, 1, "queue"},
		{"over-long queue name", "worker queues=" + strings.Repeat("q", 129) + " " + good, 1, "queue"},
		{"bad tenant name", "producer tenant=a/b " + good, 1, "tenant"},
		{"too many fields", "producer tenant=a extra " + good, 1, "fields"},
		{"only a role", "admin", 1, "fields"},
		{"duplicate hash", "producer tenant=a " + good + "\nworker queues=q " + good, 2, "duplicate"},
		{"duplicate hash across roles", "admin - " + good + "\n\n# note\nadmin - " + other + "\nproducer tenant=b " + good, 5, "line 1"},
		{"the line number counts comments and blank lines", "# one\n\n\n# four\nbogus", 5, "fields"},
		{"a bad line after good ones", "admin - " + good + "\nworker queues=q " + other + "\nbogus - " + hashOf("third"), 3, "role"},
		{"comment cuts the hash short", "admin - " + good[:10] + "#" + good[10:], 1, "64"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := LoadTokens(strings.NewReader(tc.file))
			if err == nil {
				t.Fatalf("LoadTokens accepted the file: %+v", a)
			}
			if a != nil {
				t.Errorf("LoadTokens returned an Authenticator together with an error")
			}
			wantPrefix := fmt.Sprintf("tokens file line %d: ", tc.wantLine)
			if !strings.HasPrefix(err.Error(), wantPrefix) {
				t.Fatalf("error %q does not start with %q", err, wantPrefix)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q does not mention %q", err, tc.wantText)
			}
		})
	}
}

func TestLoadTokensErrorDoesNotEchoTheHash(t *testing.T) {
	h := hashOf("secret")
	_, err := LoadTokens(strings.NewReader("producer tenant=a " + h + "\nproducer tenant=b " + h))
	if err == nil || strings.Contains(err.Error(), h) {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadTokensReaderErrors(t *testing.T) {
	boom := errors.New("disk on fire")
	_, err := LoadTokens(iotest.ErrReader(boom))
	if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "tokens file line 1: ") {
		t.Fatalf("err = %v", err)
	}

	long := "admin - " + hashOf("x") + " # " + strings.Repeat("x", 1<<20)
	_, err = LoadTokens(strings.NewReader("# fine\n" + long))
	if err == nil || !strings.HasPrefix(err.Error(), "tokens file line 2: ") {
		t.Fatalf("a line of a megabyte: err = %v", err)
	}
}

func TestUnaryAuthRequiresBearer(t *testing.T) {
	a, err := LoadTokens(strings.NewReader(
		"producer tenant=acme " + hashOf("p-acme") + "\nadmin - " + hashOf("a-root") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	intercept := UnaryAuth(a)

	call := func(md metadata.MD) (Principal, bool, error) {
		ctx := context.Background()
		if md != nil {
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		var (
			got    Principal
			called bool
		)
		_, err := intercept(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/jobqueue.v1.JobQueueService/Enqueue"},
			func(ctx context.Context, req any) (any, error) {
				called = true
				got = PrincipalFrom(ctx)
				return "resp", nil
			})
		return got, called, err
	}

	rejected := []struct {
		name string
		md   metadata.MD
	}{
		{"no metadata at all", nil},
		{"empty metadata", metadata.MD{}},
		{"other headers only", metadata.Pairs("x-token", "p-acme")},
		{"basic scheme", metadata.Pairs("authorization", "Basic cC1hY21l")},
		{"token without scheme", metadata.Pairs("authorization", "p-acme")},
		{"scheme without token", metadata.Pairs("authorization", "Bearer")},
		{"scheme with an empty token", metadata.Pairs("authorization", "Bearer ")},
		{"unknown token", metadata.Pairs("authorization", "Bearer nope")},
		{"the hash instead of the token", metadata.Pairs("authorization", "Bearer "+hashOf("p-acme"))},
		{"token with trailing space", metadata.Pairs("authorization", "Bearer p-acme ")},
		{"two authorization headers", metadata.Pairs("authorization", "Bearer p-acme", "authorization", "Bearer a-root")},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, called, err := call(tc.md)
			if called {
				t.Fatal("the handler ran")
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("code = %v (%v), want Unauthenticated", status.Code(err), err)
			}
		})
	}

	accepted := []struct {
		name string
		md   metadata.MD
		want Principal
	}{
		{"producer", metadata.Pairs("authorization", "Bearer p-acme"), Principal{Role: RoleProducer, Tenant: "acme"}},
		{"admin", metadata.Pairs("authorization", "Bearer a-root"), Principal{Role: RoleAdmin}},
		{"scheme in lower case", metadata.Pairs("authorization", "bearer a-root"), Principal{Role: RoleAdmin}},
		{"header name in capitals", metadata.Pairs("Authorization", "Bearer a-root"), Principal{Role: RoleAdmin}},
	}
	for _, tc := range accepted {
		t.Run("accepts "+tc.name, func(t *testing.T) {
			got, called, err := call(tc.md)
			if err != nil || !called {
				t.Fatalf("called = %v, err = %v", called, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("principal = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestUnaryAuthWithoutTokensMakesEveryCallerAdmin(t *testing.T) {
	intercept := UnaryAuth(nil)
	for _, md := range []metadata.MD{nil, metadata.Pairs("authorization", "Bearer whatever")} {
		ctx := context.Background()
		if md != nil {
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		var got Principal
		resp, err := intercept(ctx, "req", &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			got = PrincipalFrom(ctx)
			return "resp", nil
		})
		if err != nil || resp != "resp" {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		if got.Role != RoleAdmin {
			t.Fatalf("role = %v, want admin", got.Role)
		}
	}
}

func TestPrincipalFromBareContextIsZero(t *testing.T) {
	if p := PrincipalFrom(context.Background()); p.Role != 0 || p.Tenant != "" || p.Queues != nil {
		t.Fatalf("PrincipalFrom(bare) = %+v", p)
	}
}

// allowedRoles is the table of the brief, written out a second time: which
// roles may call which method.
var allowedRoles = map[string][]Role{
	"Enqueue":      {RoleProducer, RoleAdmin},
	"Lease":        {RoleWorker, RoleAdmin},
	"Heartbeat":    {RoleWorker, RoleAdmin},
	"Ack":          {RoleWorker, RoleAdmin},
	"Release":      {RoleWorker, RoleAdmin},
	"GetJob":       {RoleProducer, RoleAdmin},
	"CancelJob":    {RoleProducer, RoleAdmin},
	"GetStats":     {RoleAdmin},
	"ListDeadJobs": {RoleAdmin},
	"RequeueJob":   {RoleAdmin},
	"PurgeJobs":    {RoleAdmin},
}

func TestAuthorizeTable(t *testing.T) {
	principals := []Principal{
		{Role: RoleProducer, Tenant: "acme"},
		{Role: RoleWorker, Queues: []string{"q1"}},
		{Role: RoleAdmin},
		{}, // no role: a context that never went through UnaryAuth
		{Role: Role(42)},
	}
	for method, roles := range allowedRoles {
		for _, p := range principals {
			err := Authorize(p, method)
			if slices.Contains(roles, p.Role) {
				if err != nil {
					t.Errorf("Authorize(%s, %s) = %v, want nil", p.Role, method, err)
				}
				continue
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("Authorize(%s, %s) = %v, want PermissionDenied", p.Role, method, err)
				continue
			}
			if msg := err.Error(); !strings.Contains(msg, p.Role.String()) || !strings.Contains(msg, method) {
				t.Errorf("Authorize(%s, %s) message %q must name the role and the method", p.Role, method, msg)
			}
		}
	}
}

func TestAuthorizeUnknownMethodIsDenied(t *testing.T) {
	for _, m := range []string{"", "Nope", "enqueue", "/jobqueue.v1.JobQueueService/Enqueue"} {
		err := Authorize(Principal{Role: RoleAdmin}, m)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("Authorize(admin, %q) = %v, want PermissionDenied", m, err)
		}
	}
}

// FuzzLoadTokens feeds arbitrary text to LoadTokens. It never panics, and a
// file it accepts yields principals that each have a valid role and the
// binding of that role.
func FuzzLoadTokens(f *testing.F) {
	h1, h2, h3 := hashOf("one"), hashOf("two"), hashOf("three")
	for _, seed := range []string{
		"",
		"producer tenant=a " + h1,
		"worker queues=b,a " + h2 + "\nadmin - " + h3,
		"# c\n\nproducer tenant=a " + h1 + " # trailing\r\n",
		"producer tenant=a " + h1 + "\nproducer tenant=b " + h1,
		"worker queues=,, " + h1,
		"admin tenant=x " + h1,
		"operator - " + h1,
		"producer tenant=a " + strings.ToUpper(h1),
		"worker queues=a " + h1[:63],
		"\x00\xff\n\n\x00",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, file string) {
		a, err := LoadTokens(strings.NewReader(file))
		if err != nil {
			if a != nil {
				t.Fatal("an error came with an Authenticator")
			}
			if !strings.HasPrefix(err.Error(), "tokens file line ") {
				t.Fatalf("error %q lacks the line prefix", err)
			}
			return
		}
		for _, p := range a.tokens {
			switch p.Role {
			case RoleProducer:
				if err := job.ValidateName("tenant", p.Tenant); err != nil || p.Queues != nil {
					t.Errorf("producer %+v: %v", p, err)
				}
			case RoleWorker:
				if p.Tenant != "" || len(p.Queues) == 0 || !slices.IsSorted(p.Queues) || len(slices.Compact(slices.Clone(p.Queues))) != len(p.Queues) {
					t.Errorf("worker %+v: queues must be non-empty, sorted and distinct", p)
				}
				for _, q := range p.Queues {
					if err := job.ValidateName("queue", q); err != nil {
						t.Errorf("worker queue %q: %v", q, err)
					}
				}
			case RoleAdmin:
				if p.Tenant != "" || p.Queues != nil {
					t.Errorf("admin %+v carries a binding", p)
				}
			default:
				t.Errorf("principal %+v has no valid role", p)
			}
		}
	})
}
