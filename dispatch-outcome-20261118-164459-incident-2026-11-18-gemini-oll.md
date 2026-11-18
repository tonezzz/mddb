# dispatch outcome — mddb-fail-fast-writes-when-embed-provide-c55f11

Card: mddb-fail-fast-writes-when-embed-provide-c55f11 · Runner: idc02 · Date: 2026-11-18
Incident: gemini-ollama-proxy embed chain exhausted → embed queue pinned at 4000,
droppedTotal > 9300, `/v1/add` callers waited ~30s then timed out.

## What changed (code + tests only, no deploy)

Picked shape **(a)** (accept doc, vector deferred, marked) plus **(c)** (per-request
bypass flag) — both consistent with the existing #232 backpressure machinery:

- `services/mddbd/embedding_worker.go` — provider circuit breaker on
  `EmbeddingWorker`. `MDDB_EMBEDDING_CIRCUIT_FAILURES` consecutive failed Embed
  calls (default 5, `0` disables) opens the circuit for
  `MDDB_EMBEDDING_CIRCUIT_COOLDOWN` (default 30s; first success closes, first
  failure re-extends). While open: `Enqueue` drops immediately instead of
  waiting out `QUEUE_WAIT`; `processJob` declines queued jobs without calling
  the provider (backlog drains in seconds); mid-job retry loops break early.
  Drops are counted in the existing `dropped` counter with
  `reason=provider_circuit_open` in the log. Documents are always stored —
  only the vector is deferred. No embedding model/dimension changes anywhere.
- `services/mddbd/document_ops.go` — `addDocument` is now a wrapper over
  `addDocumentEx` (same signature for all ~30 existing callers), which takes a
  `skipEmbedding` flag and reports `embedDeferred`; `runPostWriteHooks` returns
  the deferred flag.
- `services/mddbd/http_handlers.go` — `handleAdd` uses `addDocumentEx`; answers
  200 promptly with `X-MDDB-Embedding: deferred` when the queue/circuit refused
  the vector job, or `skipped` when the caller passed `skipEmbedding`.
  `handleHealth` gains `embedding{queueSize,queueDepth,dropped,circuitOpen,
  consecutiveFailures,circuitRetryAfterSeconds}` — 200 stays (writes work).
- `services/mddbd/main.go` — `AddRequest.SkipEmbedding` (`skipEmbedding` JSON).
- `services/mddbd/batch_handler.go` — `AddBatchHTTPRequest.SkipEmbeddings`
  (`skipEmbeddings`) → existing `postBatchOptions.SkipEmbeddings`.
- `services/mddbd/vector_handlers.go` — `queue.circuit{state,
  consecutiveFailures, retryAfterSeconds}` in `/v1/vector-stats`.
- `services/mddbd/embedding_circuit_test.go` — 11 new tests: threshold opens,
  open-circuit Enqueue returns <2s against a 30s queue-wait, backlog drains
  with zero provider calls, real failure trips the breaker, cooldown re-probe +
  success reset, breaker disable opt-out, /v1/add stores + `deferred` header,
  skipEmbedding/skipped, batch bypass, vector-stats + health surfaces.

Docs: `CHANGELOG.md` ([Unreleased]→Added), `docs/config.md` env table,
`.env.example`, `docs/API.md`, `docs/openapi.yaml` (new fields, header, circuit
schema). Runbook: `reports/2026-11-18-embed-circuit-runbook.md` (detect → during
→ post-recovery `/v1/vector-reindex` per affected collection with force:false).

## Result

- `go build ./...` clean, `go vet .` clean, `gofmt` clean (`/tmp/go/bin`).
- Full `mddb` package suite: **ok, 109.3s** — all pre-existing tests plus the
  11 new ones. Targeted circuit/backpressure run: 1.9s.
- Healthy-path behaviour preserved: closed circuit ⇒ byte-identical request/
  response path (new header + fields only appear in degraded/bypass states).

## How to verify after deploy

- `curl -s $MDDB/health | jq .embedding` — see `circuitOpen`, `dropped`,
  `queueDepth` before writes stall.
- Simulate outage (point `MDDB_EMBEDDING_API_URL` at a dead port): writes
  return promptly, `X-MDDB-Embedding: deferred`, `queue.circuit.state=open`.
- Recovery: breaker self-closes after cooldown; then
  `POST /v1/vector-reindex {"collection":"<c>","force":false}` per collection
  that wrote during the outage (`embedded_documents < total_documents` in
  `/v1/vector-stats` shows which).

## Notes / follow-ups

- `queue.dropped` is a process counter (resets on restart); durable signal is
  `embedded_documents < total_documents`.
- gRPC/MCP/GraphQL write paths get the fail-fast for free (shared `Enqueue`),
  but only the HTTP surface carries the deferred marker.
