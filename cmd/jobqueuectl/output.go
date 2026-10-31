package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

// lastErrorWidth is the most characters of a job's last error that a table
// shows; --json and get give the whole message.
const lastErrorWidth = 60

// print writes the result of a command: v as one line of JSON under --json,
// otherwise the text that render writes. The result is written in one piece,
// after it is complete, so a command that fails never leaves half of one.
//
// The keys of every view are the names that the text output uses, in
// snake_case, so that what a person reads and what a script parses agree.
func (e *env) print(v any, render func(w *bytes.Buffer)) error {
	var out bytes.Buffer
	if e.g.json {
		enc := json.NewEncoder(&out)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(v); err != nil {
			return fmt.Errorf("encode the result: %w", err)
		}
	} else {
		render(&out)
	}
	if _, err := e.stdout.Write(out.Bytes()); err != nil {
		return fmt.Errorf("write the result: %w", err)
	}
	return nil
}

// The results of the commands, as --json prints them.

type enqueueView struct {
	ID           int64 `json:"id"`
	Deduplicated bool  `json:"deduplicated"`
}

type stateView struct {
	State string `json:"state"`
}

type requeueView struct {
	ID       int64 `json:"id"`
	Requeued bool  `json:"requeued"`
}

type purgeView struct {
	Deleted int64 `json:"deleted"`
}

type migrateView struct {
	Applied int `json:"applied"`
	Version int `json:"version"`
}

type tokenView struct {
	Token string `json:"token"`
	Line  string `json:"line"`
}

type statsView struct {
	Queue   string            `json:"queue"`
	Tenants []tenantStatsView `json:"tenants"`
}

type tenantStatsView struct {
	Tenant           string `json:"tenant"`
	Ready            int64  `json:"ready"`
	Scheduled        int64  `json:"scheduled"`
	Leased           int64  `json:"leased"`
	Dead             int64  `json:"dead"`
	OldestReadyAgeMS int64  `json:"oldest_ready_age_ms"`
}

type deadPageView struct {
	Jobs        []jobView `json:"jobs"`
	NextAfterID int64     `json:"next_after_id"`
}

// jobView is a job without its payload and result, which can be 256 KiB and
// 64 KiB of anything: it carries their lengths. A time that the job does not
// have is null.
type jobView struct {
	ID             int64   `json:"id"`
	Queue          string  `json:"queue"`
	Tenant         string  `json:"tenant"`
	State          string  `json:"state"`
	PayloadBytes   int     `json:"payload_bytes"`
	RunAt          *string `json:"run_at"`
	Attempts       int32   `json:"attempts"`
	MaxAttempts    int32   `json:"max_attempts"`
	Epoch          int64   `json:"epoch"`
	LeaseExpiresAt *string `json:"lease_expires_at"`
	LastError      string  `json:"last_error"`
	ResultBytes    int     `json:"result_bytes"`
	CreatedAt      *string `json:"created_at"`
	FinishedAt     *string `json:"finished_at"`
	IdempotencyKey string  `json:"idempotency_key"`
}

// newJobView converts a job of the API.
func newJobView(j *jobqueuev1.Job) jobView {
	return jobView{
		ID: j.GetId(), Queue: j.GetQueue(), Tenant: j.GetTenant(), State: stateName(j.GetState()),
		PayloadBytes: len(j.GetPayload()), RunAt: timeString(j.GetRunAt()),
		Attempts: j.GetAttempts(), MaxAttempts: j.GetMaxAttempts(), Epoch: j.GetEpoch(),
		LeaseExpiresAt: timeString(j.GetLeaseExpiresAt()), LastError: j.GetLastError(),
		ResultBytes: len(j.GetResult()), CreatedAt: timeString(j.GetCreatedAt()),
		FinishedAt: timeString(j.GetFinishedAt()), IdempotencyKey: j.GetIdempotencyKey(),
	}
}

// lines are the lines of get, in the order of the JSON keys.
func (v jobView) lines() [][2]string {
	return [][2]string{
		{"id", strconv.FormatInt(v.ID, 10)},
		{"queue", clean(v.Queue)},
		{"tenant", clean(v.Tenant)},
		{"state", v.State},
		{"payload_bytes", strconv.Itoa(v.PayloadBytes)},
		{"run_at", dash(derefString(v.RunAt))},
		{"attempts", strconv.Itoa(int(v.Attempts))},
		{"max_attempts", strconv.Itoa(int(v.MaxAttempts))},
		{"epoch", strconv.FormatInt(v.Epoch, 10)},
		{"lease_expires_at", dash(derefString(v.LeaseExpiresAt))},
		{"last_error", dash(clean(v.LastError))},
		{"result_bytes", strconv.Itoa(v.ResultBytes)},
		{"created_at", dash(derefString(v.CreatedAt))},
		{"finished_at", dash(derefString(v.FinishedAt))},
		{"idempotency_key", dash(clean(v.IdempotencyKey))},
	}
}

// stateName is the lower-case name of a job state: "ready", "dead" and so on.
// The generated names are JOB_STATE_READY and the like.
func stateName(s jobqueuev1.JobState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATE_"))
}

// timeString formats a time of the API in UTC, or returns nil if it is absent.
func timeString(t *timestamppb.Timestamp) *string {
	if t == nil {
		return nil
	}
	s := t.AsTime().UTC().Format(time.RFC3339Nano)
	return &s
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// dash stands for a value that is empty, so that every column and every line
// has something in it.
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// formatAge shows how old something is: whole seconds from a second up,
// milliseconds below.
func formatAge(d time.Duration) string {
	if d >= time.Second {
		return d.Round(time.Second).String()
	}
	return d.Round(time.Millisecond).String()
}

// clean makes text that came from the broker safe to print on a terminal and
// on one line: runs of white space become one space and any other character
// that does not print becomes "?". A job's error message is whatever the
// worker wrote, escape sequences included.
func clean(s string) string {
	fields := strings.Fields(s)
	for i, f := range fields {
		fields[i] = strings.Map(func(r rune) rune {
			if unicode.IsPrint(r) {
				return r
			}
			return '?'
		}, f)
	}
	return strings.Join(fields, " ")
}

// truncate cuts s to at most n characters, ending in "..." when it cut.
func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-3]) + "..."
}

// writeTable writes a header and rows as columns of equal width, two spaces
// apart.
func writeTable(w *bytes.Buffer, header []string, rows [][]string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, strings.Join(header, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	_ = tw.Flush() // the writer is a bytes.Buffer, which cannot fail
}

// writeKeyValues writes "key: value" lines with the values in one column.
func writeKeyValues(w *bytes.Buffer, pairs [][2]string) {
	width := 0
	for _, p := range pairs {
		width = max(width, len(p[0])+2)
	}
	for _, p := range pairs {
		fmt.Fprintf(w, "%-*s%s\n", width, p[0]+":", p[1])
	}
}
