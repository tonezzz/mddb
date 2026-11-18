package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mddb/internal/embedding"
	"mddb/internal/envconf"
	"mddb/internal/metrics"
	vec "mddb/internal/vector"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// errEmbedCircuitOpen marks a chunk abandoned because the provider circuit
// opened mid-job — distinct from a provider error so the "after all attempts"
// log line says what actually happened.
var errEmbedCircuitOpen = errors.New("embedding provider circuit open")

// EmbeddingJob represents a document that needs embedding.
type EmbeddingJob struct {
	Collection string
	DocID      string
	ContentMD  string
	// ChunkMode must match what retrieval uses to re-derive the passage
	// (CODE-003). Chunks are not stored, only their index — so if the two
	// disagree, chunk 3 at read time is not chunk 3 at write time and every
	// passage is wrong.
	ChunkMode ChunkMode
}

// EmbeddingWorker processes embedding jobs asynchronously.
type EmbeddingWorker struct {
	provider     embedding.Provider
	vectorStore  *vec.VectorStore
	vectorIndex  *vec.VectorIndex
	jobs         chan EmbeddingJob
	wg           sync.WaitGroup
	stopCh       chan struct{}
	chunkSize    int
	chunkEnabled bool
	metrics      *metrics.Metrics

	// enqueueWait is how long a writer waits for room in a full queue before
	// the job is abandoned. It is the difference between an import that takes
	// longer than expected and one that quietly embeds a fraction of what it
	// wrote (#232): a full queue drains continuously, so waiting turns a bulk
	// load into backpressure rather than loss. Zero restores the old
	// drop-immediately behaviour for anyone who wants ingest latency fixed.
	enqueueWait time.Duration

	// dropped counts jobs the queue could not take. It exists because the
	// information was being computed and thrown away: Enqueue has always
	// returned false on a drop, no caller has ever read it, and the only
	// trace was a log line. A number an operator can read — in
	// /v1/vector-stats — is what makes "1,002 of 4,000 embedded" a state
	// rather than a mystery.
	dropped atomic.Uint64

	// Provider circuit breaker. When the embed provider is down hard —
	// rate-limited, out of quota, unreachable — every queued job still burns
	// its full retry budget before failing, the queue stays pinned at its
	// cap, and every writer pays the entire enqueueWait before its job is
	// abandoned anyway. During the 2026-11-18 provider outage that wait was
	// the difference between a write and a timeout: /v1/add callers hung
	// ~30s and died before the document landed.
	//
	// The breaker counts consecutive failed Embed calls. At
	// circuitThreshold it opens for circuitCooldown: Enqueue drops fast
	// instead of waiting, and processJob declines jobs without touching the
	// provider, so a saturated queue drains instead of backing up writes.
	// The document itself is always stored — what is deferred is the vector,
	// recoverable afterwards with POST /v1/vector-reindex.
	consecFailures   atomic.Int64
	circuitOpenUntil atomic.Int64 // unixnano; open while now < this
	circuitThreshold int64
	circuitCooldown  time.Duration

	// indexer receives every chunk the worker embeds, and drops the chunks a
	// shrunken document no longer has. The server sets it to all of its
	// indexes; left alone it is the flat index, which is what tests of the
	// worker in isolation want.
	indexer chunkIndexer
}

// chunkIndexer is where freshly embedded chunks go.
type chunkIndexer interface {
	IndexChunk(collection, chunkKey string, vector []float32) error
	Remove(collection, chunkKey string)
}

// flatIndexer is the worker's view of a lone flat index.
type flatIndexer struct{ index *vec.VectorIndex }

func (f flatIndexer) IndexChunk(collection, chunkKey string, vector []float32) error {
	return f.index.Add(collection, chunkKey, vector)
}

func (f flatIndexer) Remove(collection, chunkKey string) { f.index.Remove(collection, chunkKey) }

// SetIndexer replaces where embedded chunks go. The server passes every index
// it keeps, so a document embedded after startup reaches HNSW, IVF and the
// rest instead of the flat index alone.
func (w *EmbeddingWorker) SetIndexer(ix chunkIndexer) {
	w.indexer = ix
}

// NewEmbeddingWorker creates a new background embedding worker.
func NewEmbeddingWorker(provider embedding.Provider, store *vec.VectorStore, index *vec.VectorIndex, bufferSize int) *EmbeddingWorker {
	threshold, cooldown := embeddingCircuitConfig()
	return &EmbeddingWorker{
		provider:         provider,
		vectorStore:      store,
		vectorIndex:      index,
		jobs:             make(chan EmbeddingJob, bufferSize),
		stopCh:           make(chan struct{}),
		chunkSize:        envconf.Int("MDDB_EMBEDDING_CHUNK_SIZE", 1500),
		chunkEnabled:     envconf.String("MDDB_EMBEDDING_CHUNK_ENABLED", "true") == "true",
		enqueueWait:      embeddingEnqueueWait(),
		circuitThreshold: threshold,
		circuitCooldown:  cooldown,
		indexer:          flatIndexer{index},
	}
}

// EmbeddingQueueSize is how many pending jobs the worker will hold.
//
// It was 1000, hardcoded at both construction sites, which is fine until an
// import is larger than it — and then there was no lever at all.
func EmbeddingQueueSize() int {
	size := envconf.Int("MDDB_EMBEDDING_QUEUE_SIZE", 1000)
	if size < 1 {
		size = 1
	}
	return size
}

// embeddingEnqueueWait reads how long a writer may wait for queue space.
//
// The default is deliberately not zero. A document written without its vector
// is invisible to every kind of search MDDB offers, and nothing about the
// write says so — the failure is silent in the one place it can least afford
// to be. Waiting makes a large import slower and complete; MDDB_EMBEDDING_
// QUEUE_WAIT=0 restores the previous behaviour for an operator who would
// rather have the latency and watch the counter.
func embeddingEnqueueWait() time.Duration {
	raw := strings.TrimSpace(envconf.String("MDDB_EMBEDDING_QUEUE_WAIT", "5s"))
	if raw == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		slog.Warn("MDDB_EMBEDDING_QUEUE_WAIT is not a duration; using the default",
			"value", raw, "default", "5s")
		return 5 * time.Second
	}
	return d
}

// embeddingCircuitConfig reads the provider circuit breaker's two knobs.
//
// MDDB_EMBEDDING_CIRCUIT_FAILURES is how many consecutive provider calls may
// fail before the worker stops asking. It counts calls, not jobs: a single
// document that burns through its retries is most of the way there already.
// Zero or a negative value disables the breaker, restoring the behaviour of
// waiting out every provider outage — the field exists for an operator who
// would rather have writes block than lose vectors.
//
// MDDB_EMBEDDING_CIRCUIT_COOLDOWN is how long the breaker stays open before
// jobs flow again. The first success closes it for real; the first failure
// reopens it, so a flapping provider produces bursts of attempts with quiet
// stretches between, not a steady trickle of 30-second stalls.
func embeddingCircuitConfig() (threshold int64, cooldown time.Duration) {
	threshold = int64(envconf.Int("MDDB_EMBEDDING_CIRCUIT_FAILURES", 5))
	raw := strings.TrimSpace(envconf.String("MDDB_EMBEDDING_CIRCUIT_COOLDOWN", "30s"))
	cooldown = 30 * time.Second
	if raw != "" {
		if d, err := time.ParseDuration(raw); err != nil || d < 0 {
			slog.Warn("MDDB_EMBEDDING_CIRCUIT_COOLDOWN is not a duration; using the default",
				"value", raw, "default", "30s")
		} else {
			cooldown = d
		}
	}
	return threshold, cooldown
}

// CircuitOpen reports whether the provider circuit is open — embedding jobs
// are being declined without reaching the provider. Read by Enqueue,
// processJob, and the stats endpoints that show an operator why vectors are
// not arriving.
func (w *EmbeddingWorker) CircuitOpen() bool {
	until := w.circuitOpenUntil.Load()
	return until != 0 && time.Now().UnixNano() < until
}

// CircuitStats reports the breaker state for /v1/vector-stats and /health:
// whether it is open, the consecutive-failure streak that opened it, and how
// long until jobs flow again (zero when closed).
func (w *EmbeddingWorker) CircuitStats() (open bool, consecutiveFailures int64, retryAfter time.Duration) {
	until := w.circuitOpenUntil.Load()
	if until != 0 {
		if d := time.Until(time.Unix(0, until)); d > 0 {
			return true, w.consecFailures.Load(), d
		}
	}
	return false, w.consecFailures.Load(), 0
}

// noteEmbedSuccess closes the failure streak — one good call means the
// provider is back, and the next cooldown expiry does not need proof.
func (w *EmbeddingWorker) noteEmbedSuccess() {
	w.consecFailures.Store(0)
}

// noteEmbedFailure counts a failed provider call and opens the circuit when
// the streak reaches the threshold. Reaching the threshold again while open
// extends the window, which is what keeps a still-down provider from being
// retried the moment the cooldown ends.
func (w *EmbeddingWorker) noteEmbedFailure(err error) {
	if w.circuitThreshold <= 0 {
		return // breaker disabled
	}
	n := w.consecFailures.Add(1)
	if n >= w.circuitThreshold && !w.CircuitOpen() {
		w.circuitOpenUntil.Store(time.Now().Add(w.circuitCooldown).UnixNano())
		slog.Warn("embedding provider circuit open: writes will be stored without vectors until the provider recovers",
			"consecutiveFailures", n, "cooldown", w.circuitCooldown,
			"err", err,
			"remedy", "restore the provider, then POST /v1/vector-reindex for the dropped documents")
	} else if n >= w.circuitThreshold {
		// Still failing while open: push the re-probe out. A fixed window
		// measured from the first failure would let a persistently-down
		// provider be retried mid-outage.
		w.circuitOpenUntil.Store(time.Now().Add(w.circuitCooldown).UnixNano())
	}
}

// QueueStats reports what the queue is holding and what it has lost.
func (w *EmbeddingWorker) QueueStats() (size, depth int, dropped uint64, wait time.Duration) {
	return cap(w.jobs), len(w.jobs), w.dropped.Load(), w.enqueueWait
}

// Start begins processing embedding jobs in the background.
func (w *EmbeddingWorker) Start(workers int) {
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}
	slog.Info("embedding worker started",
		"workers", workers, "buffer", cap(w.jobs), "chunking", w.chunkEnabled, "chunkSize", w.chunkSize)
}

// Stop gracefully stops the worker.
func (w *EmbeddingWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

// Enqueue adds an embedding job to the queue, waiting for room if the queue is
// full. Returns false only when the job was abandoned, which the caller is
// expected to report rather than ignore (#232).
func (w *EmbeddingWorker) Enqueue(job EmbeddingJob) bool {
	// Provider circuit open: the job would sit behind a queue the worker is
	// draining without embeddings anyway, so there is nothing to wait for.
	// Dropping fast is what keeps a write endpoint from spending its whole
	// enqueueWait discovering the provider is still down (2026-11-18).
	if w.CircuitOpen() {
		return w.dropJob(job, "provider_circuit_open")
	}

	// Fast path: room now, which is every write on an idle server.
	select {
	case w.jobs <- job:
		return true
	default:
	}

	if w.enqueueWait > 0 {
		timer := time.NewTimer(w.enqueueWait)
		defer timer.Stop()
		select {
		case w.jobs <- job:
			return true
		case <-timer.C:
		case <-w.stopCh:
			// Shutting down: the job is not going to be processed, and
			// blocking here would hold up the stop.
		}
	}

	return w.dropJob(job, "queue_full")
}

// dropJob records an abandoned job and always reports failure.
func (w *EmbeddingWorker) dropJob(job EmbeddingJob, reason string) bool {
	w.dropped.Add(1)
	if w.metrics != nil {
		w.metrics.IncOp("embedding", "dropped")
	}
	slog.Warn("embedding job dropped — this document will have no vector until it is reindexed",
		"reason", reason,
		"collection", job.Collection, "docID", job.DocID,
		"queueSize", cap(w.jobs), "waited", w.enqueueWait, "droppedTotal", w.dropped.Load())
	return false
}

func (w *EmbeddingWorker) worker(id int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			// Drain remaining jobs
			for {
				select {
				case job := <-w.jobs:
					w.processJob(job)
				default:
					return
				}
			}
		case job := <-w.jobs:
			w.processJob(job)
		}
	}
}

func (w *EmbeddingWorker) processJob(job EmbeddingJob) {
	contentHash := vec.ContentHash(job.ContentMD)

	// Check if content already has a matching embedding
	existing, err := w.vectorStore.Get(job.Collection, job.DocID)
	if err == nil && existing != nil && existing.ContentHash == contentHash {
		return // embedding is up-to-date
	}

	// Circuit open: the provider is down and this job can only fail after its
	// full retry budget. Decline it like a queue drop — counted, logged,
	// reindexable — so a backed-up queue drains in seconds instead of
	// burning 30s+ per document on a provider that cannot answer.
	if w.CircuitOpen() {
		w.dropJob(job, "provider_circuit_open")
		return
	}

	// Split into chunks if enabled
	var chunks []string
	if w.chunkEnabled {
		chunks = chunkTextsMode(job.ContentMD, w.chunkSize, job.ChunkMode)
	} else {
		chunks = []string{job.ContentMD}
	}

	if len(chunks) == 0 {
		return
	}

	// RAG-003: reuse the vectors of chunks whose text did not change.
	//
	// The document-level check above only helps when nothing changed at all.
	// Editing one paragraph of a fifty-chunk document changed the document
	// hash and re-embedded all fifty — at full provider cost — even though
	// forty-nine were identical. Keyed by chunk hash, so a chunk that merely
	// shifted position is still recognised.
	reusable := w.vectorStore.ChunkVectorsByHash(job.Collection, job.DocID)

	// Generate embedding for each chunk
	var chunkEmbeddings []vec.ChunkEmbedding
	var reusedCount int
	for i, chunk := range chunks {
		chunkHash := vec.ContentHash(chunk)
		if cached, ok := reusable[chunkHash]; ok {
			chunkEmbeddings = append(chunkEmbeddings, vec.ChunkEmbedding{
				ChunkIndex: i,
				Vector:     cached,
				ChunkHash:  chunkHash,
			})
			reusedCount++
			continue
		}

		var vector []float32
		var embedErr error
		for attempt := 0; attempt < 3; attempt++ {
			// A failure partway through the retries may have opened the
			// circuit: stop spending sleeps on a job that is already lost.
			if w.CircuitOpen() {
				embedErr = errEmbedCircuitOpen
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			vector, embedErr = w.provider.Embed(ctx, chunk, embedding.RoleDocument)
			cancel()
			if embedErr == nil {
				w.noteEmbedSuccess()
				break
			}
			w.noteEmbedFailure(embedErr)
			slog.Warn("embedding attempt failed", "attempt", attempt+1, "collection", job.Collection, "docID", job.DocID, "i", i, "err", embedErr)
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}

		if embedErr != nil {
			slog.Error("embedding failed after all attempts", "collection", job.Collection, "docID", job.DocID, "i", i, "err", embedErr)
			if w.metrics != nil {
				w.metrics.IncOp("embedding", "error")
			}
			return
		}

		chunkEmbeddings = append(chunkEmbeddings, vec.ChunkEmbedding{
			ChunkIndex: i,
			Vector:     vector,
			ChunkHash:  chunkHash,
		})
	}

	if reusedCount > 0 {
		slog.Debug("reused unchanged chunk embeddings",
			"collection", job.Collection, "docID", job.DocID,
			"reused", reusedCount, "embedded", len(chunks)-reusedCount)
	}

	// Store all chunks in BoltDB
	noteEmbeddingProvenance(w.vectorStore, job.Collection, w.provider)
	if err := w.vectorStore.PutChunks(job.Collection, job.DocID, chunkEmbeddings, w.provider.Model(), contentHash); err != nil {
		slog.Error("failed to store embedding", "collection", job.Collection, "docID", job.DocID, "err", err)
		return
	}

	// Update the in-memory indexes — all of them, through the indexer. Which
	// ones a collection gets (disk-only collections keep only the quantized
	// index in RAM) is the indexer's decision, made once for every writer.
	for _, ce := range chunkEmbeddings {
		chunkKey := fmt.Sprintf("%s#%d", job.DocID, ce.ChunkIndex)
		if err := w.indexer.IndexChunk(job.Collection, chunkKey, ce.Vector); err != nil {
			// The provider now refuses to hand back an unusable vector, so
			// this is a vector that no longer matches its collection — most
			// often an embedding model changed under a collection that was
			// not reindexed (#252). The chunk is stored and unsearchable.
			slog.Warn("embedded chunk refused by the vector index; it will not be searchable until reindexed",
				"collection", job.Collection, "chunk", chunkKey, "err", err)
			if w.metrics != nil {
				w.metrics.IncOp("embedding", "index_refused")
			}
		}
	}

	// Clean stale chunks (if the document shrank) from every index, not only
	// flat: the others went on returning passages the document no longer had.
	w.vectorStore.CleanStaleChunks(job.Collection, job.DocID, len(chunkEmbeddings), w.indexer)

	if w.metrics != nil {
		w.metrics.IncOp("embedding", "completed")
	}
}
