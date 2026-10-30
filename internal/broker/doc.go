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
// Every handler follows the same order: it validates the whole request, then
// authorizes the caller, and only then calls the store, so a request that is
// malformed or not allowed never reaches it. Handlers hold no state of their
// own; the store is the authority on jobs and on time.
package broker
