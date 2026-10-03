package main

import (
	"log/slog"
	"time"
)

// Leader-side binlog retention.
//
// BinlogConfig has carried MaxSize/MaxAge since the binlog landed, but
// nothing enforced them: Rotate existed and nobody called it, so the file
// grew unbounded — 9.2 GB on idc01 by 2026-10-03. This janitor enforces the
// config the file was already created with.
//
// keepLSN — entries below it are removed on rotation:
//   - connected followers: the lowest confirmed LSN. A follower only loses
//     entries it already applied and acknowledged.
//   - a follower whose ConfirmedLSN is 0 (connected but not yet streaming —
//     still on, or waiting for, a snapshot) is ignored: it replays nothing
//     from the file, it needs the snapshot stream instead.
//   - no followers at all: the oldest entry younger than MaxAge. A follower
//     returning after that window is refused by StreamBinlog and re-syncs
//     via snapshot — which is what MaxAge has always implied.
//
// Rotation only runs while the file exceeds MaxSize, so a quiet leader does
// not pay for rewrites.

// binlogJanitor periodically rotates the leader's binlog.
type binlogJanitor struct {
	rs       *ReplicationServer
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

// StartBinlogJanitor begins periodic binlog retention on the leader.
// No-op when called twice, when the binlog is off, or when interval <= 0.
func (rs *ReplicationServer) StartBinlogJanitor(interval time.Duration) {
	if interval <= 0 || rs.server.Binlog == nil {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.janitor != nil {
		return
	}
	j := &binlogJanitor{
		rs:       rs,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	rs.janitor = j
	go j.run()
	slog.Info("binlog retention janitor started",
		"interval", interval, "maxSize", rs.server.Binlog.MaxSize(), "maxAge", rs.server.Binlog.MaxAge())
}

// StopBinlogJanitor stops the janitor and waits for its goroutine to exit.
func (rs *ReplicationServer) StopBinlogJanitor() {
	rs.mu.Lock()
	j := rs.janitor
	rs.janitor = nil
	rs.mu.Unlock()
	if j != nil {
		close(j.stop)
		<-j.done
	}
}

func (j *binlogJanitor) run() {
	defer close(j.done)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			j.rotate()
		case <-j.stop:
			return
		}
	}
}

// rotate trims the binlog to keepLSN when the file exceeds MaxSize.
func (j *binlogJanitor) rotate() {
	bl := j.rs.server.Binlog
	stats := bl.Stats()
	if stats.FileSize <= bl.MaxSize() {
		return
	}

	j.rs.mu.RLock()
	var keep uint64
	followerBounded := false
	for _, f := range j.rs.followers {
		if f.ConfirmedLSN == 0 {
			continue // still snapshotting; it replays nothing from the file
		}
		if !followerBounded || f.ConfirmedLSN < keep {
			keep = f.ConfirmedLSN
			followerBounded = true
		}
	}
	j.rs.mu.RUnlock()

	if !followerBounded {
		cutoff, err := bl.OldestLSNAfter(time.Now().Add(-bl.MaxAge()))
		if err != nil {
			slog.Warn("binlog retention: age scan failed", "err", err)
			return
		}
		keep = cutoff // 0 = every entry is older than MaxAge
	}

	if keep != 0 && keep <= stats.OldestLSN {
		slog.Info("binlog over size cap but nothing trimmable",
			"fileSize", stats.FileSize, "oldestLSN", stats.OldestLSN, "keepLSN", keep)
		return
	}

	slog.Info("rotating binlog",
		"fileSize", stats.FileSize, "oldestLSN", stats.OldestLSN,
		"keepFromLSN", keep, "followerBounded", followerBounded)
	if err := bl.Rotate(keep); err != nil {
		slog.Warn("binlog rotation failed", "err", err)
		return
	}
	slog.Info("binlog rotated", "newSize", bl.Stats().FileSize)
}
