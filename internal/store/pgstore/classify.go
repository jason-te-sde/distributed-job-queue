package pgstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jason-te-sde/distributed-job-queue/internal/store"
)

// classify wraps an error from the database for the caller of operation op.
// A cancelled or expired context is the caller's doing, so it is wrapped as it
// is and is never reported as unavailability. A failure of the connection, a
// pool that has been closed, and a server that is shutting down, starting up
// or out of connections wrap store.ErrUnavailable, which the caller may
// retry. Anything else is wrapped unchanged.
func classify(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// The deadline error is also a net.Error, so this case comes first.
		return fmt.Errorf("pgstore %s: %w", op, err)
	case unavailable(err):
		// The cause is kept as text only: a caller matches on the sentinel,
		// not on a driver error type.
		return fmt.Errorf("pgstore %s: %w: %s", op, store.ErrUnavailable, err.Error())
	}
	return fmt.Errorf("pgstore %s: %w", op, err)
}

// unavailable reports whether err says that the database cannot be reached or
// is not accepting work right now.
func unavailable(err error) bool {
	var (
		connectErr *pgconn.ConnectError
		netErr     net.Error
		pgErr      *pgconn.PgError
	)
	switch {
	case errors.As(err, &connectErr), errors.As(err, &netErr), pgconn.SafeToRetry(err), closedPool(err):
		return true
	case errors.As(err, &pgErr):
		// Class 08 is connection exceptions; 57P01 to 57P03 are an
		// administrator shutdown, a crash shutdown and a server that cannot
		// accept connections yet; 53300 is too many connections.
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", "53300":
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08")
	}
	return false
}

// closedPool reports whether err is what a pgxpool.Pool returns once it has
// been closed. The pool's own error value lives in a package that this module
// may not import (it is a dependency of pgx, not one of ours), so the message
// is compared instead; TestPGClassifiesConnectionErrors fails if a pgx upgrade
// changes it.
func closedPool(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if err.Error() == "closed pool" {
			return true
		}
	}
	return false
}
