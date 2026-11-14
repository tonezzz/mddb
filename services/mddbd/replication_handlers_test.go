package main

import (
	"mddb/internal/binlog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	json "mddb/internal/jsonx"
)

// ---------- 1. handleReplicationStatus - standalone (empty role) ----------

func TestHandleReplicationStatus_Standalone(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = ""
	s.NodeID = "node-test-1"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Role != "standalone" {
		t.Errorf("expected role standalone, got %q", resp.Role)
	}
	if resp.NodeID != "node-test-1" {
		t.Errorf("expected node ID node-test-1, got %q", resp.NodeID)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true")
	}
}

// ---------- 2. handleReplicationStatus - followers is empty array not nil ----------

func TestHandleReplicationStatus_FollowersEmptyArray(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = ""

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// In standalone mode there should be no followers.
	if len(resp.Followers) != 0 {
		t.Errorf("expected 0 followers, got %d", len(resp.Followers))
	}

	// Verify the response does not contain unexpected follower data
	body := rec.Body.String()
	if strings.Contains(body, "follower_id") {
		t.Errorf("standalone response should not contain follower_id, got %s", body)
	}
}

// ---------- 3. handleReplicationStatus - leader role without binlog ----------

func TestHandleReplicationStatus_LeaderNoBinlog(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-1"
	s.Binlog = nil

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Role != "leader" {
		t.Errorf("expected role leader, got %q", resp.Role)
	}
	// Without binlog, CurrentLSN should be 0
	if resp.CurrentLSN != 0 {
		t.Errorf("expected CurrentLSN=0 without binlog, got %d", resp.CurrentLSN)
	}
}

// ---------- 4. handleReplicationStatus - follower role ----------

func TestHandleReplicationStatus_Follower(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "follower"
	s.NodeID = "follower-1"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Role != "follower" {
		t.Errorf("expected role follower, got %q", resp.Role)
	}
	if resp.NodeID != "follower-1" {
		t.Errorf("expected node ID follower-1, got %q", resp.NodeID)
	}
}

// ---------- 4b. handleReplicationStatus - follower health reflects replication ----------

func TestHandleReplicationStatus_FollowerHealthyReflectsReplication(t *testing.T) {
	// A follower whose replication client exists but has never synced must not
	// report healthy — this is the fresh-follower-at-LSN-0 case.
	t.Run("no client is unhealthy", func(t *testing.T) {
		s, cleanup := newHandlerTestServer(t)
		defer cleanup()
		s.ReplicationRole = "follower"
		s.NodeID = "follower-1"

		rec := httptest.NewRecorder()
		s.handleReplicationStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil))

		var resp ReplicationStatusResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Healthy {
			t.Error("follower with no replication client should be unhealthy")
		}
	})

	t.Run("LSN 0 is unhealthy", func(t *testing.T) {
		s, cleanup := newHandlerTestServer(t)
		defer cleanup()
		s.ReplicationRole = "follower"
		s.NodeID = "follower-1"
		rc := NewReplicationClient(s, ReplicationClientConfig{LeaderAddr: "leader:11024", FollowerID: "follower-1"})
		s.replClient = rc

		rec := httptest.NewRecorder()
		s.handleReplicationStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil))

		var resp ReplicationStatusResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Healthy {
			t.Error("follower at LSN 0 should be unhealthy")
		}
		if resp.CurrentLSN != 0 {
			t.Errorf("expected current_lsn 0, got %d", resp.CurrentLSN)
		}
		if resp.LeaderAddr != "leader:11024" {
			t.Errorf("expected leader_addr reported, got %q", resp.LeaderAddr)
		}
	})

	t.Run("caught up is healthy", func(t *testing.T) {
		s, cleanup := newHandlerTestServer(t)
		defer cleanup()
		s.ReplicationRole = "follower"
		s.NodeID = "follower-1"
		rc := NewReplicationClient(s, ReplicationClientConfig{LeaderAddr: "leader:11024", FollowerID: "follower-1"})
		rc.connected.Store(true)
		rc.applier.SetLastAppliedLSN(1234)
		rc.lagMs.Store(50)
		s.replClient = rc

		rec := httptest.NewRecorder()
		s.handleReplicationStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil))

		var resp ReplicationStatusResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !resp.Healthy {
			t.Error("a caught-up follower should be healthy")
		}
		if resp.CurrentLSN != 1234 {
			t.Errorf("expected current_lsn 1234, got %d", resp.CurrentLSN)
		}
		if resp.ReplicationLag != 50 {
			t.Errorf("expected replication_lag_ms 50, got %d", resp.ReplicationLag)
		}
	})

	t.Run("lagging beyond threshold is unhealthy", func(t *testing.T) {
		s, cleanup := newHandlerTestServer(t)
		defer cleanup()
		s.ReplicationRole = "follower"
		s.NodeID = "follower-1"
		rc := NewReplicationClient(s, ReplicationClientConfig{
			LeaderAddr: "leader:11024",
			FollowerID: "follower-1",
			MaxLag:     30 * time.Second,
		})
		rc.connected.Store(true)
		rc.applier.SetLastAppliedLSN(1234)
		rc.lagMs.Store(60_000)
		s.replClient = rc

		rec := httptest.NewRecorder()
		s.handleReplicationStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil))

		var resp ReplicationStatusResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Healthy {
			t.Error("a follower lagging beyond maxLag should be unhealthy")
		}
	})
}

// ---------- 5. handleReplicationStatus - uptime is positive ----------

func TestHandleReplicationStatus_UptimePositive(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = ""

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Uptime < 0 {
		t.Errorf("expected uptime >= 0, got %d", resp.Uptime)
	}
}

// ---------- 6. handleReplicationStatus - leader with replServer and followers ----------

func TestHandleReplicationStatus_LeaderWithFollowers(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-2"

	// Create a binlog in a temp dir
	binlogPath := t.TempDir() + "/test.binlog"
	bl, err := binlog.NewBinlog(binlogPath, binlog.BinlogConfig{Path: binlogPath})
	if err != nil {
		t.Fatalf("create binlog: %v", err)
	}
	defer func() { _ = bl.Close() }()
	s.Binlog = bl

	// Create a replication server with mock followers
	rs := &ReplicationServer{
		server: s,
		followers: map[string]*FollowerState{
			"follower-a": {
				ID:           "follower-a",
				Address:      "192.168.1.10:11024",
				ConfirmedLSN: 0,
				LastSeenAt:   time.Now().Unix(),
			},
		},
		mu: sync.RWMutex{},
	}
	s.replServer = rs

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Role != "leader" {
		t.Errorf("expected role leader, got %q", resp.Role)
	}
	if len(resp.Followers) != 1 {
		t.Fatalf("expected 1 follower, got %d", len(resp.Followers))
	}
	if resp.Followers[0].FollowerID != "follower-a" {
		t.Errorf("expected follower ID follower-a, got %q", resp.Followers[0].FollowerID)
	}
	if resp.Followers[0].Address != "192.168.1.10:11024" {
		t.Errorf("expected address 192.168.1.10:11024, got %q", resp.Followers[0].Address)
	}
}

// ---------- 7. handleReplicationStatus - follower lag status healthy ----------

func TestHandleReplicationStatus_FollowerLagHealthy(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-3"

	binlogPath := t.TempDir() + "/test.binlog"
	bl, err := binlog.NewBinlog(binlogPath, binlog.BinlogConfig{Path: binlogPath})
	if err != nil {
		t.Fatalf("create binlog: %v", err)
	}
	defer func() { _ = bl.Close() }()
	s.Binlog = bl

	// Follower with 0 lag (same LSN as leader)
	currentLSN := bl.CurrentLSN()
	rs := &ReplicationServer{
		server: s,
		followers: map[string]*FollowerState{
			"follower-b": {
				ID:           "follower-b",
				Address:      "10.0.0.1:11024",
				ConfirmedLSN: currentLSN,
				LastSeenAt:   time.Now().Unix(),
			},
		},
		mu: sync.RWMutex{},
	}
	s.replServer = rs

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Followers) != 1 {
		t.Fatalf("expected 1 follower, got %d", len(resp.Followers))
	}
	if resp.Followers[0].Status != "healthy" {
		t.Errorf("expected status healthy, got %q", resp.Followers[0].Status)
	}
	if resp.Followers[0].LagMs != 0 {
		t.Errorf("expected 0 lag, got %d", resp.Followers[0].LagMs)
	}
}

// ---------- 8. handleReplicationStatus - leader without replServer ----------

func TestHandleReplicationStatus_LeaderNoReplServer(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-4"

	binlogPath := t.TempDir() + "/test.binlog"
	bl, err := binlog.NewBinlog(binlogPath, binlog.BinlogConfig{Path: binlogPath})
	if err != nil {
		t.Fatalf("create binlog: %v", err)
	}
	defer func() { _ = bl.Close() }()
	s.Binlog = bl
	s.replServer = nil

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// No replServer means no followers, should be empty array
	if len(resp.Followers) != 0 {
		t.Errorf("expected 0 followers without replServer, got %d", len(resp.Followers))
	}
}

// ---------- 9. handleReplicationStatus - response includes binlog stats ----------

func TestHandleReplicationStatus_BinlogStats(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-5"

	binlogPath := t.TempDir() + "/test.binlog"
	bl, err := binlog.NewBinlog(binlogPath, binlog.BinlogConfig{Path: binlogPath})
	if err != nil {
		t.Fatalf("create binlog: %v", err)
	}
	defer func() { _ = bl.Close() }()
	s.Binlog = bl

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// binlog.BinlogStats should be populated (at least CurrentLSN >= 0)
	// BinlogSize >= 0 (file may have header)
	if resp.BinlogSize < 0 {
		t.Errorf("expected BinlogSize >= 0, got %d", resp.BinlogSize)
	}
}

// ---------- 10. handleReplicationStatus - multiple followers ----------

func TestHandleReplicationStatus_MultipleFollowers(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	s.ReplicationRole = "leader"
	s.NodeID = "leader-6"

	binlogPath := t.TempDir() + "/test.binlog"
	bl, err := binlog.NewBinlog(binlogPath, binlog.BinlogConfig{Path: binlogPath})
	if err != nil {
		t.Fatalf("create binlog: %v", err)
	}
	defer func() { _ = bl.Close() }()
	s.Binlog = bl

	rs := &ReplicationServer{
		server: s,
		followers: map[string]*FollowerState{
			"f1": {ID: "f1", Address: "10.0.0.1:11024", ConfirmedLSN: 0, LastSeenAt: time.Now().Unix()},
			"f2": {ID: "f2", Address: "10.0.0.2:11024", ConfirmedLSN: 0, LastSeenAt: time.Now().Unix()},
			"f3": {ID: "f3", Address: "10.0.0.3:11024", ConfirmedLSN: 0, LastSeenAt: time.Now().Unix()},
		},
		mu: sync.RWMutex{},
	}
	s.replServer = rs

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/replication/status", nil)
	s.handleReplicationStatus(rec, req)

	var resp ReplicationStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Followers) != 3 {
		t.Errorf("expected 3 followers, got %d", len(resp.Followers))
	}
}
