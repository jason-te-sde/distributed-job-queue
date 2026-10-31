package exp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pgSettings are the PostgreSQL settings that a measurement is reported with.
// They are the ones that decide durability and memory, which is what a
// throughput number depends on besides the hardware.
var pgSettings = []string{
	"synchronous_commit", "fsync", "full_page_writes", "wal_sync_method", "wal_level",
	"shared_buffers", "effective_cache_size", "work_mem", "max_wal_size", "checkpoint_timeout",
	"max_connections", "autovacuum", "autovacuum_naptime",
}

// CaptureMachine describes the machine and the toolchain: operating system,
// architecture, CPU model and count, Go version. It never fails; what it cannot
// find it reports as "unknown".
func CaptureMachine() map[string]string {
	return map[string]string{
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"cpu_model":  cpuModel(),
		"cpu_count":  strconv.Itoa(runtime.NumCPU()),
		"go_version": runtime.Version(),
		"gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)),
	}
}

// CaptureEnv is CaptureMachine plus the version of the PostgreSQL server, the
// settings in pgSettings, and the storage options of the jobs table, if there is one. A query
// that fails is recorded in the result under its own key, so that a missing
// setting is never mistaken for a default.
func CaptureEnv(ctx context.Context, pool *pgxpool.Pool) map[string]string {
	env := CaptureMachine()
	var version string
	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		env["postgres_error"] = err.Error()
		return env
	}
	env["postgres_version"] = version

	// current_setting prints a value as SHOW does, with the unit it is shown in,
	// which is what a reader of the raw output compares it with; pg_settings holds
	// the number in units of its own, such as 8kB for shared_buffers.
	rows, err := pool.Query(ctx, "SELECT name, current_setting(name) FROM unnest($1::text[]) AS name", pgSettings)
	if err != nil {
		env["postgres_settings_error"] = err.Error()
		return env
	}
	defer rows.Close()
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			env["postgres_settings_error"] = err.Error()
			return env
		}
		env["postgres_"+name] = value
	}
	if err := rows.Err(); err != nil {
		env["postgres_settings_error"] = err.Error()
	}
	rows.Close()

	var options string
	// The jobs table may not exist: a run of the River experiment, for one, uses
	// a database that no broker has touched.
	err = pool.QueryRow(ctx, `
		SELECT CASE WHEN t.oid IS NULL THEN 'no jobs table' ELSE coalesce(array_to_string(c.reloptions, ','), '') END
		  FROM (SELECT to_regclass('jobqueue.jobs') AS oid) t LEFT JOIN pg_class c ON c.oid = t.oid`).Scan(&options)
	if err != nil {
		env["jobs_table_options_error"] = err.Error()
	} else {
		env["jobs_table_options"] = options
	}
	return env
}

// cpuModel returns the model name of the CPU: from sysctl on macOS and from
// /proc/cpuinfo on Linux.
func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			if model := strings.TrimSpace(string(out)); model != "" {
				return model
			}
		}
	case "linux":
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			if model := parseCPUInfo(data); model != "" {
				return model
			}
		}
	}
	return fmt.Sprintf("unknown (%s/%s)", runtime.GOOS, runtime.GOARCH)
}

// parseCPUInfo returns the first "model name" of /proc/cpuinfo, or else the
// first "Model", which is what some ARM kernels print. It returns "" if there
// is neither.
func parseCPUInfo(data []byte) string {
	var fallback string
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		key, value, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "model name":
			return strings.TrimSpace(value)
		case "Model":
			if fallback == "" {
				fallback = strings.TrimSpace(value)
			}
		}
	}
	return fallback
}
