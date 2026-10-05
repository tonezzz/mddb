package main

import (
	"net/http"
	"time"
)

// ReplicationStatusResponse is the JSON response for /v1/replication/status
type ReplicationStatusResponse struct {
	NodeID         string             `json:"node_id"`
	Role           string             `json:"role"` // leader, follower, standalone
	CurrentLSN     uint64             `json:"current_lsn"`
	BinlogOldest   uint64             `json:"binlog_oldest_lsn,omitempty"`
	BinlogSize     int64              `json:"binlog_size_bytes,omitempty"`
	LeaderAddr     string             `json:"leader_addr,omitempty"`
	ReplicationLag int64              `json:"replication_lag_ms,omitempty"`
	Healthy        bool               `json:"healthy"`
	Followers      []FollowerInfoJSON `json:"followers,omitempty"`
	Uptime         int64              `json:"uptime_seconds"`
}

// FollowerInfoJSON is the JSON representation of a connected follower
type FollowerInfoJSON struct {
	FollowerID   string `json:"follower_id"`
	Address      string `json:"address"`
	ConfirmedLSN uint64 `json:"confirmed_lsn"`
	LagMs        int64  `json:"lag_ms"`
	LastSeenAt   int64  `json:"last_seen_at"`
	Status       string `json:"status"` // healthy, warning, unhealthy
}

var startTime = time.Now()

func (s *Server) handleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	role := s.ReplicationRole
	if role == "" {
		role = "standalone"
	}

	resp := ReplicationStatusResponse{
		NodeID:  s.NodeID,
		Role:    role,
		Healthy: true,
		Uptime:  int64(time.Since(startTime).Seconds()),
	}

	if s.Binlog != nil {
		stats := s.Binlog.Stats()
		resp.CurrentLSN = stats.CurrentLSN
		resp.BinlogOldest = stats.OldestLSN
		resp.BinlogSize = stats.FileSize
	}

	// Follower: healthy must reflect replication state — a fresh follower
	// reports healthy:true while sitting at LSN 0 otherwise.
	if s.ReplicationRole == "follower" {
		if rc := s.replClient; rc != nil {
			resp.LeaderAddr = rc.leaderAddr
			resp.CurrentLSN = rc.LastAppliedLSN()
			resp.ReplicationLag = rc.LagMs()
			resp.Healthy = rc.IsHealthy()
		} else {
			// Configured as a follower with no client running.
			resp.Healthy = false
		}
	}

	// Leader: list connected followers from ReplicationServer
	if s.ReplicationRole == "leader" && s.Binlog != nil {
		// We need access to the replication server's follower state.
		// The gRPC service is registered, so we use a package-level reference.
		if replServer := s.replServer; replServer != nil {
			replServer.mu.RLock()
			for _, fs := range replServer.followers {
				lagMs := int64(0)
				currentLSN := s.Binlog.CurrentLSN()
				if currentLSN > fs.ConfirmedLSN {
					lagMs = int64(currentLSN-fs.ConfirmedLSN) * 10 // #nosec G115 -- LSN diff within int64 range
				}

				status := "healthy"
				if lagMs > 30000 {
					status = "unhealthy"
				} else if lagMs > 1000 {
					status = "warning"
				}

				resp.Followers = append(resp.Followers, FollowerInfoJSON{
					FollowerID:   fs.ID,
					Address:      fs.Address,
					ConfirmedLSN: fs.ConfirmedLSN,
					LagMs:        lagMs,
					LastSeenAt:   fs.LastSeenAt,
					Status:       status,
				})
			}
			replServer.mu.RUnlock()
		}
	}

	if resp.Followers == nil {
		resp.Followers = []FollowerInfoJSON{}
	}

	ok(w, resp)
}
