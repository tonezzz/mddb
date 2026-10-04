package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"mddb/internal/binlog"
	proto "mddb/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The binlog stream a follower tails (#267 comment). Three ways it lost or
// withheld entries, each without an error on either side: the whole history
// was read into memory before the first entry was sent; an entry appended
// between that read and the subscription reached neither; and a follower
// that fell more than the subscription buffer behind had the excess dropped.

// recordingStream records what the leader sends and lets a test act on each
// send — append more entries, or hold the stream to make it fall behind.
type recordingStream struct {
	proto.MDDBReplication_StreamBinlogServer
	ctx    context.Context
	mu     sync.Mutex
	lsns   []uint64
	onSend func(n int)
}

func (s *recordingStream) Context() context.Context { return s.ctx }

func (s *recordingStream) Send(e *proto.BinlogEntryProto) error {
	s.mu.Lock()
	s.lsns = append(s.lsns, e.Lsn)
	n := len(s.lsns)
	s.mu.Unlock()
	if s.onSend != nil {
		s.onSend(n)
	}
	return nil
}

func (s *recordingStream) received() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.lsns...)
}

func appendN(t *testing.T, bl *binlog.Binlog, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := bl.Append(&binlog.BinlogEntry{Type: binlog.BinlogPut, BucketName: "docs", Key: []byte(fmt.Sprint(i)), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
}

// runBinlogStream starts a stream from fromLSN and waits until it has sent
// through want. fromLSN must not be below the binlog's oldest entry: those
// requests — a fresh follower's 0 included — are refused so the follower
// takes the snapshot path instead.
func runBinlogStream(t *testing.T, rs *ReplicationServer, id string, fromLSN uint64, stream *recordingStream, want uint64) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(stream.ctx)
	stream.ctx = ctx
	errc := make(chan error, 1)
	go func() { errc <- rs.StreamBinlog(&proto.StreamBinlogRequest{FollowerId: id, FromLsn: fromLSN}, stream) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		got := stream.received()
		if len(got) > 0 && got[len(got)-1] >= want {
			break
		}
		if time.Now().After(deadline) {
			cancelCtx()
			t.Fatalf("stream reached LSN %v, want %d", lastOf(got), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cancelCtx, errc
}

func lastOf(l []uint64) uint64 {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1]
}

// assertContiguous checks the follower got every LSN from first to last,
// once, in order — the property replication depends on.
func assertContiguous(t *testing.T, got []uint64, first, last uint64) {
	t.Helper()
	if uint64(len(got)) != last-first+1 {
		t.Fatalf("received %d entries, want %d..%d", len(got), first, last)
	}
	for i, lsn := range got {
		if want := first + uint64(i); lsn != want {
			t.Fatalf("entry %d has LSN %d, want %d — the follower has a gap", i, lsn, want)
		}
	}
}

func TestEntriesWrittenDuringTheReplayReachTheFollower(t *testing.T) {
	s, bl, cleanup := replTestServer(t)
	defer cleanup()
	rs := NewReplicationServer(s)
	appendN(t, bl, 100)

	stream := &recordingStream{ctx: authedReplCtx(t, "s")}
	var once sync.Once
	stream.onSend = func(int) {
		// Writes keep arriving while the history is being sent.
		once.Do(func() { appendN(t, bl, 50) })
	}
	// FromLsn 1 is the file's oldest: the replay covers 2..100, then the 50.
	cancel, done := runBinlogStream(t, rs, "f1", 1, stream, 150)
	cancel()
	<-done

	assertContiguous(t, stream.received(), 2, 150)
}

func TestAFollowerThatFallsBehindTheBufferGetsEveryEntry(t *testing.T) {
	s, bl, cleanup := replTestServer(t)
	defer cleanup()
	rs := NewReplicationServer(s)
	appendN(t, bl, 2)

	release := make(chan struct{})
	stream := &recordingStream{ctx: authedReplCtx(t, "s")}
	stream.onSend = func(n int) {
		if n == 2 {
			<-release // the follower stalls on its second entry
		}
	}
	ctx, cancelCtx := context.WithCancel(stream.ctx)
	stream.ctx = ctx
	done := make(chan error, 1)
	// FromLsn 1 is the file's oldest: the replay sends entry 2, then the
	// follower stalls and the burst below overflows its subscription buffer.
	go func() {
		done <- rs.StreamBinlog(&proto.StreamBinlogRequest{FollowerId: "f1", FromLsn: 1}, stream)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for len(stream.received()) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(stream.received()) == 0 {
		t.Fatal("stream sent nothing")
	}
	// A bulk import while the follower is stalled: more than the subscription
	// buffer holds, so the binlog drops the excess.
	const burst = 6000
	appendN(t, bl, burst)
	close(release)

	for lastOf(stream.received()) < uint64(burst)+2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancelCtx()
	<-done
	assertContiguous(t, stream.received(), 2, uint64(burst)+2)
}

// A follower that reconnects under the same ID before its old stream has
// ended: the old stream's cleanup must not take the new stream with it.
func TestAReconnectedFollowerKeepsItsStream(t *testing.T) {
	s, bl, cleanup := replTestServer(t)
	defer cleanup()
	rs := NewReplicationServer(s)
	appendN(t, bl, 2)

	old := &recordingStream{ctx: authedReplCtx(t, "s")}
	cancelOld, oldDone := runBinlogStream(t, rs, "f1", 1, old, 2)
	fresh := &recordingStream{ctx: authedReplCtx(t, "s")}
	cancelFresh, freshDone := runBinlogStream(t, rs, "f1", 1, fresh, 2)
	defer func() { cancelFresh(); <-freshDone }()

	cancelOld()
	<-oldDone

	if got := bl.Stats().Subscribers; got != 1 {
		t.Errorf("subscribers after the old stream ended = %d, want 1", got)
	}
	rs.mu.RLock()
	_, tracked := rs.followers["f1"]
	rs.mu.RUnlock()
	if !tracked {
		t.Error("the old stream's cleanup removed the reconnected follower")
	}

	appendN(t, bl, 1)
	deadline := time.Now().Add(5 * time.Second)
	for lastOf(fresh.received()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if lastOf(fresh.received()) != 3 {
		t.Errorf("the reconnected stream stopped receiving: %v", fresh.received())
	}
}

// A follower asking below the file's oldest entry — a fresh follower's LSN 0
// included — is sent to the snapshot path, not fed the whole retained log.
func TestAFollowerBelowTheOldestLSNGetsFailedPrecondition(t *testing.T) {
	s, bl, cleanup := replTestServer(t)
	defer cleanup()
	rs := NewReplicationServer(s)
	appendN(t, bl, 10)

	// Trim so the file starts at 5: both 0 and a mid-history LSN are below it.
	if err := bl.Rotate(5); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	for _, from := range []uint64{0, 4} {
		stream := &recordingStream{ctx: authedReplCtx(t, "s")}
		err := rs.StreamBinlog(&proto.StreamBinlogRequest{FollowerId: "f1", FromLsn: from}, stream)
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("FromLsn %d below oldest 5: got %v, want FailedPrecondition", from, err)
		}
	}

	// At the oldest the stream still works — the kept tail is replayed.
	stream := &recordingStream{ctx: authedReplCtx(t, "s")}
	cancel, done := runBinlogStream(t, rs, "f1", 5, stream, 10)
	cancel()
	<-done
	assertContiguous(t, stream.received(), 6, 10)
}
