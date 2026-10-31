package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jason-te-sde/distributed-job-queue/internal/core/job"
	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
	"github.com/jason-te-sde/distributed-job-queue/internal/store/pgstore/migrate"
)

// databaseURLEnv names the environment variable that supplies --database-url,
// the same one that jobqueue-server reads.
const databaseURLEnv = "JOBQUEUE_DATABASE_URL"

// tokenBytes is the length of a token before encoding: 256 bits.
const tokenBytes = 32

// noArguments fails with a usage error when a command that takes none was
// given one.
func noArguments(positional []string) error {
	if len(positional) > 0 {
		return usagef("unexpected argument %q", positional[0])
	}
	return nil
}

// requireQueue is the check that every command with a --queue flag makes.
func requireQueue(queue string) error {
	if queue == "" {
		return usagef("--queue is required")
	}
	return nil
}

// jobID reads the one argument of get, cancel and requeue: a job id.
func jobID(positional []string) (int64, error) {
	if len(positional) != 1 {
		return 0, usagef("want exactly one job id, got %d arguments", len(positional))
	}
	id, err := strconv.ParseInt(positional[0], 10, 64)
	if err != nil || id <= 0 {
		return 0, usagef("%q is not a job id: want a positive integer", positional[0])
	}
	return id, nil
}

// int32Flag converts a flag that was read as an int into the int32 that the
// API carries, so that a value that does not fit is refused instead of wrapping
// around.
func int32Flag(name string, v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, usagef("--%s %d does not fit in 32 bits", name, v)
	}
	return int32(v), nil
}

// cmdEnqueue adds one job.
func cmdEnqueue(e *env, args []string) error {
	fs := e.flagSet()
	var (
		queue, tenant, key, payloadFile, payloadText, runAt string
		delay                                               time.Duration
		maxAttempts                                         int
	)
	fs.StringVar(&queue, "queue", "", "queue to add the job to (required)")
	fs.StringVar(&tenant, "tenant", "", "tenant that owns the job (default: the token's tenant, or \"default\" for an admin)")
	fs.DurationVar(&delay, "delay", 0, "wait this long before the job becomes ready")
	fs.StringVar(&runAt, "run-at", "", "RFC 3339 time at which the job becomes ready")
	fs.IntVar(&maxAttempts, "max-attempts", 0, "attempts before the job is dead (default: the broker's)")
	fs.StringVar(&key, "key", "", "idempotency key: a job with the same key in the same queue and tenant is not added twice")
	fs.StringVar(&payloadFile, "payload", "", "payload from a file, written @<path>")
	fs.StringVar(&payloadText, "payload-string", "", "payload as literal text")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	set := setFlags(fs)

	if err := noArguments(positional); err != nil {
		return err
	}
	if err := requireQueue(queue); err != nil {
		return err
	}
	if set["delay"] && set["run-at"] {
		return usagef("--delay and --run-at both say when the job becomes ready: give one")
	}
	switch {
	case set["payload"] && set["payload-string"]:
		return usagef("--payload and --payload-string both give the payload: give one")
	case !set["payload"] && !set["payload-string"]:
		return usagef("the payload is required: give --payload @<file> or --payload-string <text>")
	}
	attempts, err := int32Flag("max-attempts", maxAttempts)
	if err != nil {
		return err
	}
	item := &jobqueuev1.EnqueueItem{Queue: queue, Tenant: tenant, MaxAttempts: attempts, IdempotencyKey: key}
	if set["delay"] {
		item.Delay = durationpb.New(delay)
	}
	if set["run-at"] {
		t, err := time.Parse(time.RFC3339, runAt)
		if err != nil {
			return usagef("--run-at %q is not an RFC 3339 time such as 2026-01-02T15:04:05Z", runAt)
		}
		item.RunAt = timestamppb.New(t)
	}
	if set["payload"] {
		path, ok := strings.CutPrefix(payloadFile, "@")
		if !ok || path == "" {
			return usagef("--payload takes @<file>; use --payload-string for literal text")
		}
		if item.Payload, err = readPayload(path); err != nil {
			return err
		}
	} else {
		item.Payload = []byte(payloadText)
	}

	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.Enqueue(s.ctx, &jobqueuev1.EnqueueRequest{Items: []*jobqueuev1.EnqueueItem{item}})
	if err != nil {
		return rpcError(err)
	}
	if len(resp.GetResults()) != 1 {
		return fmt.Errorf("the broker answered %d results for 1 job", len(resp.GetResults()))
	}
	res := resp.GetResults()[0]
	return e.print(enqueueView{ID: res.GetId(), Deduplicated: res.GetDeduplicated()}, func(w *bytes.Buffer) {
		fmt.Fprintf(w, "id=%d deduplicated=%t\n", res.GetId(), res.GetDeduplicated())
	})
}

// readPayload reads a payload file. It reads one byte past the largest payload
// the broker takes, which is enough to tell a file that is too big without
// holding all of it: the file may be a device that never ends.
func readPayload(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("--payload: %w", err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, job.MaxPayloadBytes+1))
	if err != nil {
		return nil, fmt.Errorf("--payload: %w", err)
	}
	if len(b) > job.MaxPayloadBytes {
		return nil, fmt.Errorf("--payload: %s is larger than the %d bytes that a payload may have", path, job.MaxPayloadBytes)
	}
	return b, nil
}

// cmdGet shows one job.
func cmdGet(e *env, args []string) error {
	fs := e.flagSet()
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	id, err := jobID(positional)
	if err != nil {
		return err
	}
	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.GetJob(s.ctx, &jobqueuev1.GetJobRequest{Id: id})
	if err != nil {
		return rpcError(err)
	}
	if resp.GetJob() == nil {
		return fmt.Errorf("the broker answered without a job")
	}
	v := newJobView(resp.GetJob())
	return e.print(v, func(w *bytes.Buffer) { writeKeyValues(w, v.lines()) })
}

// cmdCancel stops a job that has not finished.
func cmdCancel(e *env, args []string) error {
	fs := e.flagSet()
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	id, err := jobID(positional)
	if err != nil {
		return err
	}
	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.CancelJob(s.ctx, &jobqueuev1.CancelJobRequest{Id: id})
	if err != nil {
		return rpcError(err)
	}
	state := stateName(resp.GetState())
	return e.print(stateView{State: state}, func(w *bytes.Buffer) { fmt.Fprintf(w, "state=%s\n", state) })
}

// cmdStats counts a queue's jobs per tenant.
func cmdStats(e *env, args []string) error {
	fs := e.flagSet()
	var queue string
	fs.StringVar(&queue, "queue", "", "queue to count (required)")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	if err := requireQueue(queue); err != nil {
		return err
	}
	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.GetStats(s.ctx, &jobqueuev1.GetStatsRequest{Queue: queue})
	if err != nil {
		return rpcError(err)
	}
	v := statsView{Queue: queue, Tenants: make([]tenantStatsView, len(resp.GetTenants()))}
	for i, t := range resp.GetTenants() {
		v.Tenants[i] = tenantStatsView{
			Tenant: t.GetTenant(), Ready: t.GetReady(), Scheduled: t.GetScheduled(),
			Leased: t.GetLeased(), Dead: t.GetDead(),
		}
		if t.GetReady() > 0 {
			ms := t.GetOldestReadyAge().AsDuration().Milliseconds()
			v.Tenants[i].OldestReadyAgeMS = &ms
		}
	}
	return e.print(v, func(w *bytes.Buffer) {
		rows := make([][]string, len(v.Tenants))
		for i, t := range v.Tenants {
			oldest := "-"
			if t.OldestReadyAgeMS != nil {
				oldest = formatAge(time.Duration(*t.OldestReadyAgeMS) * time.Millisecond)
			}
			rows[i] = []string{
				clean(t.Tenant), strconv.FormatInt(t.Ready, 10), strconv.FormatInt(t.Scheduled, 10),
				strconv.FormatInt(t.Leased, 10), strconv.FormatInt(t.Dead, 10), oldest,
			}
		}
		writeTable(w, []string{"TENANT", "READY", "SCHEDULED", "LEASED", "DEAD", "OLDEST"}, rows)
	})
}

// cmdDeadList lists one page of a queue's dead jobs.
func cmdDeadList(e *env, args []string) error {
	fs := e.flagSet()
	var (
		queue string
		after int64
		limit int
	)
	fs.StringVar(&queue, "queue", "", "queue to list (required)")
	fs.Int64Var(&after, "after", 0, "list the jobs after this id; the previous page names it")
	fs.IntVar(&limit, "limit", 0, "most jobs in the page (default: the broker's)")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	if err := requireQueue(queue); err != nil {
		return err
	}
	if after < 0 {
		return usagef("--after must not be negative, got %d", after)
	}
	pageSize, err := int32Flag("limit", limit)
	if err != nil {
		return err
	}
	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.ListDeadJobs(s.ctx, &jobqueuev1.ListDeadJobsRequest{Queue: queue, AfterId: after, Limit: pageSize})
	if err != nil {
		return rpcError(err)
	}
	v := deadPageView{Jobs: make([]jobView, len(resp.GetJobs())), NextAfterID: resp.GetNextAfterId()}
	for i, j := range resp.GetJobs() {
		v.Jobs[i] = newJobView(j)
	}
	return e.print(v, func(w *bytes.Buffer) {
		rows := make([][]string, len(v.Jobs))
		for i, j := range v.Jobs {
			rows[i] = []string{
				strconv.FormatInt(j.ID, 10), clean(j.Tenant),
				fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts), dash(derefString(j.FinishedAt)),
				dash(truncate(clean(j.LastError), lastErrorWidth)),
			}
		}
		writeTable(w, []string{"ID", "TENANT", "ATTEMPTS", "FINISHED", "LAST_ERROR"}, rows)
		if v.NextAfterID != 0 {
			fmt.Fprintf(w, "next: --after %d\n", v.NextAfterID)
		}
	})
}

// cmdRequeue gives a dead job another run.
func cmdRequeue(e *env, args []string) error {
	fs := e.flagSet()
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	id, err := jobID(positional)
	if err != nil {
		return err
	}
	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	if _, err := s.client.RequeueJob(s.ctx, &jobqueuev1.RequeueJobRequest{Id: id}); err != nil {
		return rpcError(err)
	}
	return e.print(requeueView{ID: id, Requeued: true}, func(w *bytes.Buffer) {
		fmt.Fprintf(w, "id=%d requeued=true\n", id)
	})
}

// purgeStates are the states that purge accepts, which are the ones a job
// never leaves.
var purgeStates = map[string]jobqueuev1.JobState{
	"succeeded": jobqueuev1.JobState_JOB_STATE_SUCCEEDED,
	"dead":      jobqueuev1.JobState_JOB_STATE_DEAD,
	"cancelled": jobqueuev1.JobState_JOB_STATE_CANCELLED,
}

// cmdPurge deletes the finished jobs of one state that finished before a time.
func cmdPurge(e *env, args []string) error {
	fs := e.flagSet()
	var queue, state, before string
	fs.StringVar(&queue, "queue", "", "queue to purge (required)")
	fs.StringVar(&state, "state", "", "succeeded, dead or cancelled (required)")
	fs.StringVar(&before, "before", "", "RFC 3339 time; jobs that finished before it are deleted (required)")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	if err := requireQueue(queue); err != nil {
		return err
	}
	jobState, ok := purgeStates[state]
	switch {
	case state == "":
		return usagef("--state is required: succeeded, dead or cancelled")
	case !ok:
		return usagef("--state %q is not one of succeeded, dead, cancelled", state)
	case before == "":
		return usagef("--before is required: an RFC 3339 time such as 2026-01-02T15:04:05Z")
	}
	cutoff, err := time.Parse(time.RFC3339, before)
	if err != nil {
		return usagef("--before %q is not an RFC 3339 time such as 2026-01-02T15:04:05Z", before)
	}

	s, err := e.dial()
	if err != nil {
		return err
	}
	defer s.close()
	resp, err := s.client.PurgeJobs(s.ctx, &jobqueuev1.PurgeJobsRequest{
		Queue: queue, State: jobState, FinishedBefore: timestamppb.New(cutoff),
	})
	if err != nil {
		return rpcError(err)
	}
	return e.print(purgeView{Deleted: resp.GetDeleted()}, func(w *bytes.Buffer) {
		fmt.Fprintf(w, "deleted=%d\n", resp.GetDeleted())
	})
}

// cmdMigrate applies the database migrations. It talks to the database, not to
// the broker, so the shared flags that name the broker do not apply to it.
func cmdMigrate(e *env, args []string) error {
	fs := e.flagSet()
	// The URL carries the password, so it is kept out of the help text like the
	// token, and the environment supplies it when the flag does not.
	url := e.getenv(databaseURLEnv)
	fs.Var(secret{&url}, "database-url", "PostgreSQL connection URL (env "+databaseURLEnv+")")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	if url == "" {
		return usagef("--database-url or $%s is required", databaseURLEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.g.timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("migrate: open the database: %w", err)
	}
	defer pool.Close()
	applied, err := migrate.Up(ctx, pool)
	if err != nil {
		if applied > 0 {
			return fmt.Errorf("%w (%d applied before the failure)", err, applied)
		}
		return err
	}
	// Up applies what is missing but does not look at what else is there, so a
	// database that a newer binary migrated past this one would pass for
	// migrated. Check reads the version back from the database: it is nil only
	// when the highest version recorded there is migrate.Version, which is then
	// the version of the database as well as of this binary.
	if err := migrate.Check(ctx, pool); err != nil {
		var mismatch *migrate.SchemaMismatchError
		if errors.As(err, &mismatch) && mismatch.Have > mismatch.Want {
			return fmt.Errorf("migrate: the database is at schema version %d, ahead of the %d that this binary knows: use a newer jobqueuectl",
				mismatch.Have, mismatch.Want)
		}
		return fmt.Errorf("migrate: check the schema version after migrating: %w", err)
	}
	return e.print(migrateView{Applied: applied, Version: migrate.Version}, func(w *bytes.Buffer) {
		fmt.Fprintf(w, "applied=%d version=%d\n", applied, migrate.Version)
	})
}

// cmdTokenNew mints a token. The token goes to the caller and only its hash
// goes into the line for the broker's tokens file, so the broker never holds a
// credential.
func cmdTokenNew(e *env, args []string) error {
	fs := e.flagSet()
	var role, tenant, queues string
	fs.StringVar(&role, "role", "", "producer, worker or admin (required)")
	fs.StringVar(&tenant, "tenant", "", "tenant of a producer")
	fs.StringVar(&queues, "queues", "", "comma-separated queues of a worker")
	positional, err := e.parse(fs, args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	set := setFlags(fs)

	var binding string
	switch role {
	case "producer":
		if !set["tenant"] || set["queues"] {
			return usagef("a producer is bound to a tenant: give --tenant and no --queues")
		}
		if err := job.ValidateName("tenant", tenant); err != nil {
			return usagef("%v", err)
		}
		binding = "tenant=" + tenant
	case "worker":
		if !set["queues"] || set["tenant"] {
			return usagef("a worker is bound to queues: give --queues and no --tenant")
		}
		for _, q := range strings.Split(queues, ",") {
			if err := job.ValidateName("queue", q); err != nil {
				return usagef("%v", err)
			}
		}
		binding = "queues=" + queues
	case "admin":
		if set["tenant"] || set["queues"] {
			return usagef("an admin has no binding: give neither --tenant nor --queues")
		}
		binding = "-"
	case "":
		return usagef("--role is required: producer, worker or admin")
	default:
		return usagef("--role %q is not one of producer, worker, admin", role)
	}

	token, line, err := mintToken(rand.Reader, role, binding)
	if err != nil {
		return err
	}
	return e.print(tokenView{Token: token, Line: line}, func(w *bytes.Buffer) {
		fmt.Fprintf(w, "token: %s\nline: %s\n", token, line)
	})
}

// mintToken draws a token from r and returns it with the line for the tokens
// file that lists it: the role, its binding and the SHA-256 of the token, which
// is what broker.LoadTokens reads and the broker compares.
func mintToken(r io.Reader, role, binding string) (token, line string, err error) {
	raw := make([]byte, tokenBytes)
	if _, err := io.ReadFull(r, raw); err != nil {
		return "", "", fmt.Errorf("draw a random token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, role + " " + binding + " " + hex.EncodeToString(sum[:]), nil
}

// cmdVersion prints the version of this build. It talks to nothing, so the
// shared flags that name the broker do not apply to it.
func cmdVersion(e *env, args []string) error {
	positional, err := e.parse(e.flagSet(), args)
	if err != nil {
		return err
	}
	if err := noArguments(positional); err != nil {
		return err
	}
	return e.printVersion()
}

// printVersion writes the version of this build, as text or as --json.
func (e *env) printVersion() error {
	return e.print(versionView{Version: version}, func(w *bytes.Buffer) {
		fmt.Fprintln(w, version)
	})
}

// cmdSleep waits for a duration and succeeds. It is not listed in the usage
// text: the Helm chart runs it as the preStop hook of the broker's container,
// whose image has no shell, to give the endpoint time to leave the load
// balancer before the server starts to shut down.
func cmdSleep(_ *env, args []string) error {
	if len(args) != 1 {
		return usagef("want one duration, such as 5s")
	}
	d, err := time.ParseDuration(args[0])
	if err != nil || d < 0 {
		return usagef("%q is not a duration of zero or more, such as 5s", args[0])
	}
	time.Sleep(d)
	return nil
}
