package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Proving a database file is one bbolt can open (#266, #270).
//
// mddb opens its database with NoFreelistSync, so bbolt rebuilds the freelist
// on every open by walking every page. A file whose page tree is damaged —
// a backup copied while writes were landing, or a database that went bad in
// place — fails that walk with a panic:
//
//	freepages: failed to get all reachable pages (page 119988: multiple references ...)
//
// and the panic cannot be contained in the process that hits it. bbolt's walk
// runs in a goroutine that keeps reading pages through a transaction the
// panicking side has already rolled back, so it can fault on its own, where no
// recover reaches; and when it does not, it is left blocked holding a read
// transaction, which is why `bbolt compact` on such a file hangs. A restore
// that opened a damaged backup in the server process therefore did not fail —
// it left the server with its database closed and the damaged file in its
// place.
//
// So a file is proven in a child process: the server re-runs its own binary,
// which opens the file the way the server would and runs bbolt's full check.
// Whatever bbolt does to that process, the server reads an exit code.

// verifyChildEnv names the file a child process should verify. Set only by
// verifyDatabase; the child is this binary (or the test binary, whose TestMain
// honours it too).
const verifyChildEnv = "MDDB_INTERNAL_VERIFY_DB"

// verifyTimeout bounds a check. Large databases on slow disks are slow to
// walk — tens of minutes have been reported cold on spinning disks — so the
// default is generous and MDDB_VERIFY_TIMEOUT can raise it.
func verifyTimeout() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("MDDB_VERIFY_TIMEOUT")); err == nil && d > 0 {
		return d
	}
	return time.Hour
}

// verifyOpenOnlyEnv tells the checker child to stop after proving the file
// opens — freelist walk included — and skip tx.Check()'s page-by-page
// invariant walk. Set only for trusted material: a replication snapshot that
// arrived complete over the stream and was already opened on the leader. A
// restore file is untrusted by comparison and keeps the whole check.
const verifyOpenOnlyEnv = "MDDB_INTERNAL_VERIFY_OPEN_ONLY"

// verifyDatabase reports whether the file at path passes bbolt's integrity
// check, run in a child process. The error carries bbolt's own description of
// the damage.
func verifyDatabase(path string) error {
	return runVerifyChildProcess(path, false)
}

// verifySnapshot proves a replication snapshot the cheap way: the child opens
// it exactly as the server would — freelist walk included, still the open
// where a damaged page tree panics — and skips tx.Check(), the
// O(every-page) invariant walk reported at over 6h for a ~4GB snapshot on a
// contended disk.
func verifySnapshot(path string) error {
	return runVerifyChildProcess(path, true)
}

// verifyProgressInterval is how often a running walk is reported. The checks
// used to be silent for their whole run — over an hour on a large database
// under disk contention — which reads exactly like a hang.
const verifyProgressInterval = 30 * time.Second

// runVerifyChildProcess runs the checker child. openOnly skips the full
// invariant check after the open proof (see verifyOpenOnlyEnv).
func runVerifyChildProcess(path string, openOnly bool) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot run the integrity check: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), verifyTimeout())
	defer cancel()

	// #nosec G204 -- runs this binary, with the path passed in the environment
	cmd := exec.CommandContext(ctx, exe)
	cmd.Env = append(os.Environ(), verifyChildEnv+"="+path)
	if openOnly {
		cmd.Env = append(cmd.Env, verifyOpenOnlyEnv+"=1")
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot run the integrity check: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	var size int64
	if info, statErr := os.Stat(path); statErr == nil {
		size = info.Size()
	}
	started := time.Now()
	ticker := time.NewTicker(verifyProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if ctx.Err() != nil {
				return fmt.Errorf("%s: integrity check did not finish within %s (MDDB_VERIFY_TIMEOUT)", filepath.Base(path), verifyTimeout())
			}
			if err != nil {
				return fmt.Errorf("%s fails bbolt's integrity check: %s", filepath.Base(path), failureReason(out.String()))
			}
			slog.Info("database integrity check finished",
				"file", filepath.Base(path), "elapsed", time.Since(started).Round(time.Second))
			return nil
		case <-ticker.C:
			attrs := []any{"file", filepath.Base(path), "elapsed", time.Since(started).Round(time.Second)}
			if p := verifyProgress(cmd.Process.Pid, size); p != "" {
				attrs = append(attrs, "progress", p)
			}
			slog.Info("database integrity check still running", attrs...)
		}
	}
}

// verifyProgress describes the checker child's progress from /proc — Linux
// only, "" elsewhere. mmap page-ins count toward read_bytes and CPU ticks
// accrue either way, so the figures move during both the freelist walk and
// tx.Check() even though bbolt never calls read() on the file.
func verifyProgress(pid int, size int64) string {
	ioData, ioErr := os.ReadFile(fmt.Sprintf("/proc/%d/io", pid))      //nolint:gosec // G703: pid is our child's
	statData, statErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) //nolint:gosec // G703: pid is our child's
	if ioErr != nil && statErr != nil {
		return ""
	}
	var parts []string
	if statErr == nil {
		// Fields after the (comm) group: state, ppid, … utime is field 14 and
		// stime 15 of the stat line → indexes 11 and 12 here. 100 ticks/sec.
		if i := bytes.LastIndexByte(statData, ')'); i >= 0 {
			if f := bytes.Fields(statData[i+1:]); len(f) > 12 {
				utime, _ := strconv.ParseInt(string(f[11]), 10, 64)
				stime, _ := strconv.ParseInt(string(f[12]), 10, 64)
				parts = append(parts, fmt.Sprintf("cpu=%ds", (utime+stime)/100))
			}
		}
	}
	if ioErr == nil && size > 0 {
		for _, line := range strings.Split(string(ioData), "\n") {
			v, ok := strings.CutPrefix(line, "read_bytes: ")
			if !ok {
				continue
			}
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				pct := n * 100 / size
				if pct > 100 {
					pct = 100
				}
				parts = append(parts, fmt.Sprintf("read=%d%% (%d/%d MiB)", pct, n>>20, size>>20))
			}
			break
		}
	}
	return strings.Join(parts, " ")
}

// withWalkProgress runs fn while logging it every verifyProgressInterval —
// bbolt's page walks are silent for their whole run, which reads exactly like
// a hang — and reports the elapsed time at the end when it took a moment.
func withWalkProgress(what string, attrs []any, fn func() error) error {
	start := time.Now()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(verifyProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				slog.Info(what+" still running", append([]any{"elapsed", time.Since(start).Round(time.Second)}, attrs...)...)
			case <-done:
				return
			}
		}
	}()
	err := fn()
	close(done)
	if elapsed := time.Since(start); elapsed >= time.Second {
		slog.Info(what+" done", append([]any{"elapsed", elapsed.Round(time.Second)}, attrs...)...)
	}
	return err
}

// runVerifyChild is the child side: open the file as the server would, run
// the full check, report, and exit non-zero on any damage. A panic in here is
// also a failure — the parent sees the exit status and the panic text.
func runVerifyChild(path string) int {
	db, err := openBolt(path, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true, // the walk every server open performs
		FreelistType:    bolt.FreelistMapType,
		Timeout:         5 * time.Second,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = db.Close() }()

	if os.Getenv(verifyOpenOnlyEnv) != "" {
		fmt.Println("ok")
		return 0
	}

	var problems []string
	_ = db.View(func(tx *bolt.Tx) error {
		// Drained to the end: bbolt's checker blocks if its channel is left.
		for e := range tx.Check() {
			if len(problems) < 10 {
				problems = append(problems, e.Error())
			}
		}
		return nil
	})
	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, strings.Join(problems, "\n"))
		return 1
	}
	fmt.Println("ok")
	return 0
}

// persistFreelist writes a freelist into a database file that has none.
//
// mddb runs with NoFreelistSync, so a file image — a backup, a replication
// snapshot — carries no freelist and every open rebuilds it by walking every
// page. bbolt flushes a freelist into the file itself when a syncing handle
// opens an unsynced file, which is the whole of this function: the walk runs
// once, here, instead of on every later open of the copy.
func persistFreelist(path string) error {
	db, err := openBolt(path, &bolt.Options{
		NoFreelistSync: false,
		FreelistType:   bolt.FreelistMapType,
		Timeout:        5 * time.Second,
	})
	if err != nil {
		return err
	}
	return db.Close()
}

// openBolt opens a database and turns a panic inside bbolt into an error, so a
// damaged file is reported rather than taking down the goroutine that opened
// it. See the note above for why this is a last line of defence, not a check.
func openBolt(path string, opts *bolt.Options) (db *bolt.DB, err error) {
	defer func() {
		if r := recover(); r != nil {
			db, err = nil, fmt.Errorf("bbolt panicked opening %s: %v", filepath.Base(path), r)
		}
	}()
	return bolt.Open(path, 0o600, opts)
}

// failureReason picks bbolt's own words out of a child's output: the panic
// message if it crashed, its report otherwise — not a stack trace.
func failureReason(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if i := strings.Index(line, "panic: "); i >= 0 {
			return strings.TrimSpace(line[i+len("panic: "):])
		}
	}
	return lastLines(out, 6)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}
