package main

import (
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"

	"mddb/internal/binlog"
	proto "mddb/proto"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Regression tests for the replication heartbeat: the leader emits a
// LSN-less liveness entry on an idle stream, and the follower arms a recv
// deadline once it has seen one — a frozen-but-connected stream then fails
// and loop() reconnects instead of silently diverging.

// silentBinlogStream is a follower-side stream fake. It hands the test a
// channel to feed entries and blocks in Recv once that channel is quiet —
// the frozen-stream condition the deadline exists for.
type silentBinlogStream struct {
	grpc.ClientStream // embedded nil — only Recv/Context are exercised
	ctx               context.Context
	ch                chan *proto.BinlogEntryProto
}

func (s *silentBinlogStream) Recv() (*proto.BinlogEntryProto, error) {
	select {
	case e, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		return e, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// fakeReplicationClient returns the test stream for StreamBinlog. The other
// RPCs return errors — replicate() only reaches them on paths the test does
// not take.
type fakeReplicationClient struct {
	proto.MDDBReplicationClient // embedded nil — unused methods would panic
	stream                      *silentBinlogStream
}

func (c *fakeReplicationClient) StreamBinlog(ctx context.Context, _ *proto.StreamBinlogRequest, _ ...grpc.CallOption) (proto.MDDBReplication_StreamBinlogClient, error) {
	// Hand the stream the call ctx like grpc does, so the cancel replicate()
	// runs on return unblocks a Recv sitting inside it.
	c.stream.ctx = ctx
	return c.stream, nil
}

func (c *fakeReplicationClient) AcknowledgeLSN(context.Context, *proto.AcknowledgeLSNRequest, ...grpc.CallOption) (*proto.AcknowledgeLSNResponse, error) {
	return nil, status.Error(codes.Unavailable, "no acks in test")
}

func (c *fakeReplicationClient) RequestSnapshot(context.Context, *proto.SnapshotRequest, ...grpc.CallOption) (proto.MDDBReplication_RequestSnapshotClient, error) {
	return nil, status.Error(codes.Unavailable, "no snapshot in test")
}

func newHeartbeatTestClient(t *testing.T, timeout time.Duration) *ReplicationClient {
	t.Helper()
	s, cleanup := newTestServerForReplClient(t)
	t.Cleanup(cleanup)
	return NewReplicationClient(s, ReplicationClientConfig{
		LeaderAddr:       "test-leader",
		FollowerID:       "f-test",
		HeartbeatTimeout: timeout,
	})
}

// A stream that goes silent after proving heartbeat-capable must fail within
// the deadline so loop() reconnects.
func TestSilentStreamTriggersHeartbeatReconnect(t *testing.T) {
	rc := newHeartbeatTestClient(t, 120*time.Millisecond)
	stream := &silentBinlogStream{ch: make(chan *proto.BinlogEntryProto, 4)}
	rc.client = &fakeReplicationClient{stream: stream}

	// The leader proves it is heartbeat-capable, then goes silent — the
	// frozen-but-connected condition.
	stream.ch <- &proto.BinlogEntryProto{Heartbeat: true, Timestamp: time.Now().UnixNano()}

	start := time.Now()
	err := rc.replicate()
	elapsed := time.Since(start)

	if err == nil || !strings.Contains(err.Error(), "replication heartbeat timeout") {
		t.Fatalf("expected heartbeat timeout error, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("reconnect took %s, want ~%s", elapsed, rc.heartbeatTimeout)
	}
	if !rc.HeartbeatCapable() {
		t.Fatal("heartbeat was not recorded as capability")
	}
	// The heartbeat carried no LSN and was never applied.
	if lsn := rc.LastAppliedLSN(); lsn != 0 {
		t.Fatalf("heartbeat advanced the applied LSN to %d", lsn)
	}
	if rc.LastRecvAt() == 0 {
		t.Fatal("last-recv timestamp not recorded")
	}
}

// A silent stream on a leader that never sent a heartbeat (pre-heartbeat
// build) must NOT time out — otherwise every idle window flaps the
// reconnect loop.
func TestSilentPreHeartbeatStreamDoesNotTimeout(t *testing.T) {
	rc := newHeartbeatTestClient(t, 100*time.Millisecond)
	stream := &silentBinlogStream{ch: make(chan *proto.BinlogEntryProto)}
	rc.client = &fakeReplicationClient{stream: stream}

	done := make(chan error, 1)
	go func() { done <- rc.replicate() }()

	select {
	case err := <-done:
		t.Fatalf("stream without heartbeats returned early: %v", err)
	case <-time.After(4 * rc.heartbeatTimeout):
		// Still blocked on Recv — pre-heartbeat behavior preserved.
	}

	close(rc.stopCh) // stop the client
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean stop, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replicate did not stop after stopCh closed")
	}
}

// Heartbeat entries must never reach the applier — no bucket write, no LSN
// marker movement — while real entries on the same stream apply normally.
func TestHeartbeatEntriesAreNotApplied(t *testing.T) {
	rc := newHeartbeatTestClient(t, 5*time.Second)
	stream := &silentBinlogStream{ch: make(chan *proto.BinlogEntryProto, 8)}
	rc.client = &fakeReplicationClient{stream: stream}

	stream.ch <- &proto.BinlogEntryProto{Heartbeat: true, Timestamp: time.Now().UnixNano()}
	stream.ch <- &proto.BinlogEntryProto{
		Lsn:        7,
		Type:       uint32(binlog.BinlogPut),
		Timestamp:  time.Now().UnixNano(),
		BucketName: "docs",
		Key:        []byte("doc|col|k1"),
		Value:      []byte(`{"key":"k1","collection":"col"}`),
	}
	stream.ch <- &proto.BinlogEntryProto{Heartbeat: true, Timestamp: time.Now().UnixNano()}
	close(stream.ch) // EOF after the second heartbeat

	if err := rc.replicate(); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if lsn := rc.LastAppliedLSN(); lsn != 7 {
		t.Fatalf("applied LSN = %d, want 7", lsn)
	}

	// The persisted marker reflects the real entry, not the heartbeat.
	var persisted uint64
	if err := rc.server.DBView(func(tx *bolt.Tx) error {
		b := tx.Bucket(replMetaBucket)
		if b == nil {
			return nil
		}
		if v := b.Get(replMetaLSNKey); len(v) == 8 {
			persisted = binary.BigEndian.Uint64(v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if persisted != 7 {
		t.Fatalf("persisted LSN = %d, want 7", persisted)
	}
}

// A pre-heartbeat follower (e.g. idc02's 2.15.4-lsn image) receiving the
// proto a heartbeat-capable leader sends sees it as an empty entry — the
// unknown field is dropped. It must fail to apply WITHOUT writing the LSN
// marker, so an upgraded leader never wedges an old follower into a
// snapshot reset.
func TestHeartbeatProtoFailsApplyWithoutLSNWrite(t *testing.T) {
	s, cleanup := newTestServerForReplClient(t)
	defer cleanup()
	applier := NewReplicationApplier(s)

	// What an old follower decodes: heartbeat flag dropped, everything zero.
	entry := protoToEntry(&proto.BinlogEntryProto{
		Heartbeat: true,
		Timestamp: time.Now().UnixNano(),
	})
	if err := applier.Apply(entry); err == nil {
		t.Fatal("empty heartbeat entry applied cleanly on the old-follower path — it would clobber the LSN marker")
	}
	if lsn := applier.LastAppliedLSN(); lsn != 0 {
		t.Fatalf("heartbeat entry moved applied LSN to %d", lsn)
	}
	if lsn, err := applier.loadPersistedLSN(); err != nil || lsn != 0 {
		t.Fatalf("persisted LSN = %d (err %v), want 0", lsn, err)
	}
}

// The leader emits a heartbeat entry when the stream sits idle, and the
// heartbeat does not advance the stream's LSN position — real entries after
// it still arrive contiguous.
func TestLeaderSendsHeartbeatOnIdleStream(t *testing.T) {
	old := replicationHeartbeatInterval
	replicationHeartbeatInterval = 60 * time.Millisecond
	defer func() { replicationHeartbeatInterval = old }()

	s, bl, cleanup := replTestServer(t)
	defer cleanup()
	rs := NewReplicationServer(s)
	appendN(t, bl, 3)

	stream := &recordingStream{ctx: authedReplCtx(t, "s")}
	cancel, errc := runBinlogStream(t, rs, "f-hb", 1, stream, 3)
	defer cancel()

	// Wait for at least one heartbeat proto.
	deadline := time.Now().Add(5 * time.Second)
	for {
		seenHB := false
		for _, e := range stream.sentProtos() {
			if e.Heartbeat {
				seenHB = true
				break
			}
		}
		if seenHB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader sent no heartbeat on an idle stream")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, e := range stream.sentProtos() {
		if e.Heartbeat && e.Lsn != 0 {
			t.Fatalf("heartbeat carried LSN %d — it must not advance the LSN", e.Lsn)
		}
	}

	// A real write after heartbeats still arrives with the next LSN — the
	// heartbeat did not consume stream position.
	appendN(t, bl, 1)
	deadline = time.Now().Add(5 * time.Second)
	for {
		var lastDataLSN uint64
		for _, e := range stream.sentProtos() {
			if !e.Heartbeat {
				lastDataLSN = e.Lsn
			}
		}
		if lastDataLSN == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("entry after heartbeat never arrived; last data LSN %d, want 4", lastDataLSN)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The follower's last_sent_at reflects stream activity.
	rs.mu.RLock()
	fs := rs.followers["f-hb"]
	rs.mu.RUnlock()
	if fs == nil {
		t.Fatal("follower state missing")
	}
	if fs.LastSentAt.Load() == 0 {
		t.Fatal("last_sent_at never updated")
	}

	cancel()
	if err := <-errc; err != nil && err != context.Canceled {
		t.Fatalf("stream returned %v", err)
	}
}
