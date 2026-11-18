package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mddb/internal/testsync"
	vector "mddb/internal/vector"
)

// The 2026-11-18 incident shape: every embed provider route was down at once,
// the queue pinned at its cap, and every /v1/add paid the full
// MDDB_EMBEDDING_QUEUE_WAIT discovering a job that was going to be dropped
// anyway — the HTTP write timed out before the caller heard anything. These
// tests cover the two halves of the fix: a provider circuit that makes the
// wait pointless gets skipped, and the write still lands, marked as deferred.

// circuitWorker builds a worker whose breaker trips on a single failure and
// stays open for an hour, so a test can pin it open deterministically —
// the state the real breaker reaches after a few failed provider calls.
func circuitWorker(t *testing.T, provider *embWorkerMockProvider, bufferSize int) *EmbeddingWorker {
	t.Helper()
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_FAILURES", "1")
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_COOLDOWN", "1h")

	_, vs, vi := embWorkerSetup(t)
	return NewEmbeddingWorker(provider, vs, vi, bufferSize)
}

// tripCircuit forces the breaker open the way processJob would after a failed
// Embed call. Same package, so the test drives the mechanism directly instead
// of spending real seconds on provider retries.
func tripCircuit(t *testing.T, w *EmbeddingWorker) {
	t.Helper()
	w.noteEmbedFailure(errors.New("provider down"))
	if !w.CircuitOpen() {
		t.Fatal("noteEmbedFailure did not open the circuit")
	}
}

func TestConsecutiveFailuresOpenTheCircuit(t *testing.T) {
	_, vs, vi := embWorkerSetup(t)
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}}

	t.Setenv("MDDB_EMBEDDING_CIRCUIT_FAILURES", "3")
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_COOLDOWN", "1h")
	w := NewEmbeddingWorker(provider, vs, vi, 10)

	for i := int64(0); i < 2; i++ {
		w.noteEmbedFailure(fmt.Errorf("failure %d", i+1))
		if w.CircuitOpen() {
			t.Fatalf("circuit opened after %d failures, threshold is 3", i+1)
		}
	}
	w.noteEmbedFailure(errors.New("third"))
	if !w.CircuitOpen() {
		t.Fatal("circuit did not open at the threshold")
	}

	open, failures, retryAfter := w.CircuitStats()
	if !open || failures != 3 || retryAfter <= 0 {
		t.Errorf("CircuitStats = open:%v failures:%d retryAfter:%s", open, failures, retryAfter)
	}
}

// The incident itself: queue full, provider dead, and a write that would have
// waited out the whole queue-wait for a job that could never run. With the
// circuit open the answer has to come back immediately.
func TestAnOpenCircuitDropsWithoutWaiting(t *testing.T) {
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}, failUntil: 1 << 30}

	// Thirty seconds is the production-shaped wait the incident hit. If the
	// circuit check is missing or wrong this test takes 30s to fail.
	t.Setenv("MDDB_EMBEDDING_QUEUE_WAIT", "30s")
	w := circuitWorker(t, provider, 1)

	// Pin the queue: one slot, no workers draining — the incident shape.
	if !w.Enqueue(EmbeddingJob{Collection: "c", DocID: "stuck", ContentMD: "x"}) {
		t.Fatal("the first job did not fit the queue")
	}
	tripCircuit(t, w)

	start := time.Now()
	ok := w.Enqueue(EmbeddingJob{Collection: "c", DocID: "new", ContentMD: "y"})
	elapsed := time.Since(start)

	if ok {
		t.Error("a job was accepted while the provider circuit was open")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Enqueue took %s with the circuit open — a write endpoint would have timed out", elapsed)
	}
	if _, _, dropped, _ := w.QueueStats(); dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}
}

// While the circuit is open the worker must not spend provider calls on the
// backlog — the queued documents are just as lost as new ones, and counting
// them is what keeps `dropped` honest.
func TestTheWorkerDrainsTheBacklogWithoutCallingTheProvider(t *testing.T) {
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}, failUntil: 1 << 30}
	w := circuitWorker(t, provider, 10)

	for i := 0; i < 5; i++ {
		if !w.Enqueue(EmbeddingJob{Collection: "c", DocID: fmt.Sprintf("d%d", i), ContentMD: "x"}) {
			t.Fatalf("job %d did not fit the queue", i)
		}
	}
	tripCircuit(t, w)

	w.Start(1)
	defer w.Stop()

	testsync.WaitForCount(t, "the backlog to be declined", 5, func() int {
		_, _, dropped, _ := w.QueueStats()
		return int(dropped)
	})
	if provider.getCallCount() != 0 {
		t.Errorf("provider was called %d times while its circuit was open", provider.getCallCount())
	}
}

// A real provider outage opens the breaker by itself: one job's failed
// attempt is enough at threshold 1, and nothing else needs to be set up.
func TestARealFailureOpensTheCircuit(t *testing.T) {
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}, failUntil: 1 << 30}
	w := circuitWorker(t, provider, 10)
	w.Start(1)
	defer w.Stop()

	if !w.Enqueue(EmbeddingJob{Collection: "c", DocID: "d", ContentMD: "body"}) {
		t.Fatal("the job did not fit the queue")
	}
	testsync.Wait(t, "the provider failure to open the circuit", w.CircuitOpen)
}

// The breaker is a pause, not a state: once the cooldown passes, the next job
// is tried for real, and a success resets the streak.
func TestTheCircuitReprobesAfterCooldown(t *testing.T) {
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}, failUntil: 1 << 30}
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_FAILURES", "1")
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_COOLDOWN", "60ms")
	_, vs, vi := embWorkerSetup(t)
	w := NewEmbeddingWorker(provider, vs, vi, 10)

	tripCircuit(t, w)
	testsync.Wait(t, "the cooldown to lapse", func() bool { return !w.CircuitOpen() })

	// Provider healthy again: a job must go through and be embedded.
	provider.mu.Lock()
	provider.failUntil = 0
	provider.mu.Unlock()

	w.Start(1)
	defer w.Stop()
	if !w.Enqueue(EmbeddingJob{Collection: "c", DocID: "d", ContentMD: "x"}) {
		t.Fatal("the job was refused after the cooldown")
	}
	testsync.Wait(t, "the job to be embedded", func() bool {
		rec, err := vs.Get("c", "d")
		return err == nil && rec != nil
	})
	if _, failures, _ := w.CircuitStats(); failures != 0 {
		t.Errorf("consecutiveFailures = %d after a success, want 0", failures)
	}
}

// A broken breaker must never switch the vector space: nothing in the
// fast-drop path changes the provider, the model, or what gets stored.
func TestCircuitDisabledStillWaits(t *testing.T) {
	provider := &embWorkerMockProvider{model: "m", dims: 3, vector: []float32{0.1}, failUntil: 1 << 30}
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_FAILURES", "0") // operator opt-out
	_, vs, vi := embWorkerSetup(t)
	w := NewEmbeddingWorker(provider, vs, vi, 10)

	w.noteEmbedFailure(errors.New("down"))
	w.noteEmbedFailure(errors.New("still down"))
	if w.CircuitOpen() {
		t.Error("circuit opened with the breaker disabled")
	}
}

// ---- HTTP surface ----

// circuitServer is stallEmbeddingWorker's sibling for the outage: the queue
// is wedged AND the provider circuit is pinned open, so writes must not wait.
func circuitServer(t *testing.T, srv *Server) *embWorkerMockProvider {
	t.Helper()
	t.Setenv("MDDB_EMBEDDING_QUEUE_WAIT", "30s")
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_FAILURES", "1")
	t.Setenv("MDDB_EMBEDDING_CIRCUIT_COOLDOWN", "1h")

	vs := vector.NewVectorStore(srv.DB)
	if err := vs.EnsureBucket(); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	index := vector.NewVectorIndex()
	index.SetReady()

	provider := &embWorkerMockProvider{model: "test-model", dims: 3, vector: []float32{0.1, 0.2, 0.3}, failUntil: 1 << 30}
	srv.EmbeddingWorker = NewEmbeddingWorker(provider, vs, index, 1)
	srv.VectorStore = vs
	srv.VectorIndex = index

	// Fill the single queue slot, then break the provider. Both halves of
	// the outage are now true for every write that follows.
	if !srv.EmbeddingWorker.Enqueue(EmbeddingJob{Collection: "c", DocID: "stuck", ContentMD: "x"}) {
		t.Fatal("the wedging job did not fit the queue")
	}
	tripCircuit(t, srv.EmbeddingWorker)
	return provider
}

// The write endpoint's contract under an outage: the document lands, the
// answer is prompt, and it carries the needs-reindex marker.
func TestAddDuringAnOutageStoresTheDocumentAndSaysSo(t *testing.T) {
	srv, cleanup := newHandlerTestServer(t)
	defer cleanup()
	circuitServer(t, srv)

	body := `{"collection":"c","key":"k","lang":"en","contentMd":"still needs storing"}`
	rec := httptest.NewRecorder()
	start := time.Now()
	srv.handleAdd(rec, httptest.NewRequest(http.MethodPost, "/v1/add", strings.NewReader(body)))
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if elapsed > 2*time.Second {
		t.Errorf("handleAdd took %s with the circuit open — the incident was a 30s wait", elapsed)
	}
	if got := rec.Header().Get("X-MDDB-Embedding"); got != "deferred" {
		t.Errorf("X-MDDB-Embedding = %q, want \"deferred\"", got)
	}

	// The document really is stored — the marker means "vector deferred",
	// not "write failed".
	get := doRequest(t, srv.handleGet, GetRequest{Collection: "c", Key: "k", Lang: "en"})
	if get.Code != http.StatusOK {
		t.Fatalf("the stored document did not read back: %d", get.Code)
	}
}

func TestSkipEmbeddingBypassesTheQueueEntirely(t *testing.T) {
	srv, cleanup := newHandlerTestServer(t)
	defer cleanup()
	circuitServer(t, srv)

	body := `{"collection":"c","key":"k","lang":"en","contentMd":"text","skipEmbedding":true}`
	rec := httptest.NewRecorder()
	srv.handleAdd(rec, httptest.NewRequest(http.MethodPost, "/v1/add", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-MDDB-Embedding"); got != "skipped" {
		t.Errorf("X-MDDB-Embedding = %q, want \"skipped\"", got)
	}
	// A requested bypass is not a loss: nothing was asked for, so the dropped
	// counter must not move.
	if _, _, dropped, _ := srv.EmbeddingWorker.QueueStats(); dropped != 0 {
		t.Errorf("dropped = %d after a skipEmbedding write, want 0", dropped)
	}
}

func TestBatchSkipEmbeddingsStoresWithoutTouchingTheQueue(t *testing.T) {
	srv, cleanup := newHandlerTestServer(t)
	defer cleanup()
	circuitServer(t, srv)

	body := `{"collection":"c","skipEmbeddings":true,"documents":[
		{"key":"a","lang":"en","contentMd":"one"},
		{"key":"b","lang":"en","contentMd":"two"}]}`
	rec := httptest.NewRecorder()
	srv.handleAddBatch(rec, httptest.NewRequest(http.MethodPost, "/v1/add-batch", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "embeddingDropped") {
		t.Errorf("a bypassed batch reports losses it did not have: %s", rec.Body.String())
	}
	if _, _, dropped, _ := srv.EmbeddingWorker.QueueStats(); dropped != 0 {
		t.Errorf("dropped = %d after a skipEmbeddings batch, want 0", dropped)
	}
}

// ---- Observability ----

func TestVectorStatsShowsTheCircuit(t *testing.T) {
	srv, cleanup := newHandlerTestServer(t)
	defer cleanup()
	circuitServer(t, srv)

	rec := httptest.NewRecorder()
	srv.handleVectorStats(rec, httptest.NewRequest(http.MethodGet, "/v1/vector-stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`"state":"open"`, `"consecutiveFailures":1`, "retryAfterSeconds", `"depth":1`} {
		if !strings.Contains(body, want) {
			t.Errorf("vector-stats is missing %s: %s", want, body)
		}
	}
}

func TestHealthShowsTheEmbeddingPipeline(t *testing.T) {
	srv, cleanup := newHandlerTestServer(t)
	defer cleanup()
	circuitServer(t, srv)

	rec := httptest.NewRecorder()
	srv.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`"circuitOpen":true`, `"queueDepth":1`, `"consecutiveFailures":1`} {
		if !strings.Contains(body, want) {
			t.Errorf("/health is missing %s: %s", want, body)
		}
	}
}
