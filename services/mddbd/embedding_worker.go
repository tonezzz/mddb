package main

import (
	"context"
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
	// Attempts counts processings that ended in a provider error, including
	// outage recirculations — it bounds a job's total life so a document
	// the provider always rejects cannot recirculate forever.
	Attempts int
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

	// retried counts jobs handed back to the queue after a provider error
	// (2026-11-12: before retry existed, a failed job was abandoned silently
	// — the doc lost its vector with no trace until a full reindex).
	retried atomic.Uint64

	// maxJobAttempts bounds a job's total life: every failed processing
	// requeues it (provider outage included — the document is not at fault
	// during an outage and must survive it), but past this budget it drops
	// so a permanently-poisoned document cannot hold the breaker open
	// forever while hiding behind free recirculation.
	maxJobAttempts int

	// indexer receives every chunk the worker embeds, and drops the chunks a
	// shrunken document no longer has. The server sets it to all of its
	// indexes; left alone it is the flat index, which is what tests of the
	// worker in isolation want.
	indexer chunkIndexer

	// breaker pauses embed attempts while the provider looks dead.
	// Without it a dead upstream turns a full queue into hours of
	// retries×timeout burn — the 2026-11-08 bge-lessons-shadow storm held
	// a core at ~90% for 7.5h doing exactly that.
	breaker *embedBreaker
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
	return &EmbeddingWorker{
		provider:       provider,
		vectorStore:    store,
		vectorIndex:    index,
		jobs:           make(chan EmbeddingJob, bufferSize),
		stopCh:         make(chan struct{}),
		chunkSize:      envconf.Int("MDDB_EMBEDDING_CHUNK_SIZE", 1500),
		chunkEnabled:   envconf.String("MDDB_EMBEDDING_CHUNK_ENABLED", "true") == "true",
		enqueueWait:    embeddingEnqueueWait(),
		indexer:        flatIndexer{index},
		breaker:        newEmbedBreaker(),
		maxJobAttempts: maxEmbeddingJobAttempts(),
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

// QueueStats reports what the queue is holding and what it has lost.
func (w *EmbeddingWorker) QueueStats() (size, depth int, dropped, retried uint64, wait time.Duration) {
	return cap(w.jobs), len(w.jobs), w.dropped.Load(), w.retried.Load(), w.enqueueWait
}

// maxEmbeddingJobAttempts is how many failed processings a job survives
// before it is dropped. The default is generous — at the breaker's max
// 10-minute cooldown it covers roughly an eight-hour provider outage —
// while still bounded so a permanently rejected document dies instead of
// recirculating forever and holding the breaker open.
func maxEmbeddingJobAttempts() int {
	n := envconf.Int("MDDB_EMBEDDING_JOB_MAX_ATTEMPTS", 50)
	if n < 1 {
		n = 1
	}
	return n
}

// embedBreaker is a provider-outage circuit breaker for the whole
// embedding queue. Each failed Embed call counts; past the threshold the
// circuit opens for an exponential cooldown (base, doubling to max).
// Workers sleep out the cooldown instead of every queued document burning
// retries×timeout against a dead upstream — the 2026-11-08
// bge-lessons-shadow storm was 7.5h of exactly that. On expiry the next
// attempt is a half-open trial: any failure re-opens at doubled backoff,
// a success fully resets.
type embedBreaker struct {
	consecutive atomic.Int64
	until       atomic.Int64 // unixnano deadline while the circuit is open
	backoff     atomic.Int64 // current cooldown (ns); >0 means tripped
	threshold   int64
	base        time.Duration
	max         time.Duration
}

func newEmbedBreaker() *embedBreaker {
	b := &embedBreaker{
		threshold: envconf.Int64("MDDB_EMBEDDING_BREAKER_THRESHOLD", 8),
		base:      embedBreakerDuration("MDDB_EMBEDDING_BREAKER_BASE", 30*time.Second),
		max:       embedBreakerDuration("MDDB_EMBEDDING_BREAKER_MAX", 10*time.Minute),
	}
	if b.base < time.Second {
		b.base = time.Second
	}
	if b.max < b.base {
		b.max = b.base
	}
	return b
}

func embedBreakerDuration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(envconf.String(key, def.String()))
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn(key+" is not a positive duration; using the default",
			"value", raw, "default", def)
		return def
	}
	return d
}

// deadline is when an open circuit next allows a trial; zero when closed.
func (b *embedBreaker) deadline() time.Time {
	return time.Unix(0, b.until.Load())
}

// fail records one failed Embed call. The first trip needs `threshold`
// consecutive failures; once tripped, any failure re-opens immediately
// with doubled backoff until a success resets.
func (b *embedBreaker) fail() {
	if b == nil || b.threshold <= 0 {
		return // breaker disabled
	}
	n := b.consecutive.Add(1)
	tripped := b.backoff.Load() > 0
	if !tripped && n < b.threshold {
		return
	}
	bo := time.Duration(b.backoff.Load())
	if bo < b.base {
		bo = b.base
	} else {
		bo *= 2
	}
	if bo > b.max {
		bo = b.max
	}
	b.backoff.Store(int64(bo))
	b.until.Store(time.Now().Add(bo).UnixNano())
	b.consecutive.Store(0)
	slog.Warn("embedding provider circuit open — pausing embed attempts",
		"backoff", bo, "consecutiveFailures", n)
}

// succeed closes the circuit: counters and backoff reset.
func (b *embedBreaker) succeed() {
	if b == nil {
		return
	}
	b.consecutive.Store(0)
	b.backoff.Store(0)
}

// waitBreaker sleeps until the circuit deadline, returning false when the
// worker is stopping so the caller abandons the job instead of sleeping.
func (w *EmbeddingWorker) waitBreaker(until time.Time) bool {
	d := time.Until(until)
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.stopCh:
		return false
	}
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

	return w.dropJob(job)
}

// requeue hands a failed job back to the tail of the queue without
// waiting — a retried job must never block a worker behind fresh writes,
// and when the queue is full the retried job drops first so new documents
// always win the last slot.
func (w *EmbeddingWorker) requeue(job EmbeddingJob) bool {
	select {
	case w.jobs <- job:
		w.retried.Add(1)
		if w.metrics != nil {
			w.metrics.IncOp("embedding", "retried")
		}
		return true
	default:
		return w.dropJob(job)
	}
}

// dropJob records an abandoned job and always reports failure.
func (w *EmbeddingWorker) dropJob(job EmbeddingJob) bool {
	w.dropped.Add(1)
	if w.metrics != nil {
		w.metrics.IncOp("embedding", "dropped")
	}
	slog.Warn("embedding queue full, dropping job — this document will have no vector until it is reindexed",
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

		// Circuit breaker: while the provider is systematically down the
		// circuit is open — sleep it out instead of burning retries×30s
		// timeouts on every queued document (2026-11-08 embed storm).
		if until := w.breaker.deadline(); until.After(time.Now()) {
			if !w.waitBreaker(until) {
				return // worker stopping
			}
		}

		var vector []float32
		var embedErr error
		for attempt := 0; attempt < 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			vector, embedErr = w.provider.Embed(ctx, chunk, embedding.RoleDocument)
			cancel()
			if embedErr == nil {
				w.breaker.succeed()
				break
			}
			slog.Warn("embedding attempt failed", "attempt", attempt+1, "collection", job.Collection, "docID", job.DocID, "i", i, "err", embedErr)
			w.breaker.fail()
			if w.breaker.deadline().After(time.Now()) {
				break // circuit just opened — stop burning attempts on this chunk
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}

		if embedErr != nil {
			job.Attempts++
			if job.Attempts < w.maxJobAttempts {
				slog.Warn("embedding failed, requeueing job",
					"collection", job.Collection, "docID", job.DocID, "i", i,
					"attempts", job.Attempts, "maxAttempts", w.maxJobAttempts, "err", embedErr)
				w.requeue(job)
			} else {
				slog.Error("embedding job dropped after max attempts — document will have no vector until reindexed",
					"collection", job.Collection, "docID", job.DocID,
					"attempts", job.Attempts, "err", embedErr)
				w.dropJob(job)
			}
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
