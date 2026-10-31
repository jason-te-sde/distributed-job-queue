// Package broker is the server side of the queue: it turns what clients send
// over gRPC into calls on a store.Store, and what the store answers into
// what clients receive.
//
// The package holds the shell around the pure core. Configuration (Config,
// ParseFlags) comes from flags and the environment. Callers are identified by
// static bearer tokens: LoadTokens reads a file of token hashes, UnaryAuth
// turns the Authorization header into a Principal, and Authorize says which
// roles may call which method. toStatus is the one place where errors of the
// store and the core become gRPC status codes.
//
// Server implements the gRPC API. Every handler follows the same order: it
// validates the whole request, then authorizes the caller, and only then calls
// the store, so a request that is malformed or not allowed never reaches it.
// Apart from a jitter generator and a draining flag the Server keeps no state:
// the store is the authority on jobs and on time. Waiting for work, and
// sharing it between tenants, is the job of a LeaseDispatcher that Lease
// calls. Metrics holds the Prometheus collectors that the handlers update.
//
// Dispatcher is the LeaseDispatcher of the broker. It is the shell around the
// pure dispatch core: one goroutine owns the core and the fair-share planners,
// carries out the effects the core returns, and talks to the store through
// goroutines of its own that report back. Listener feeds it the notifications
// of the store.
//
// Scaler answers KEDA's external scaler protocol from the backlog of a queue.
// It has a gRPC server of its own, without the authentication interceptor,
// because KEDA cannot send tokens.
//
// Broker assembles everything: the gRPC server behind the authentication
// interceptor, the dispatcher and listener, the reaper and pruner (RunReaper,
// RunPruner), the HTTP endpoint with /healthz, /readyz and /metrics (Health,
// Metrics), the Scaler's server when ScalerAddr is set, and the ordered
// shutdown. cmd/jobqueue-server is a thin command around New and Run.
package broker
