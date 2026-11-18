# mddbd embed-provider circuit breaker — incident runbook

Date: 2026-11-18
Card: mddb-fail-fast-writes-when-embed-provide-c55f11 (runner idc02)
Incident: 2026-11-18 gemini-ollama-proxy embed chain exhausted
(Gemini 429, OpenRouter total-limit 403, paid hourly-cap). mddb-cms embed
queue pinned at 4000, droppedTotal > 9300. Every `/v1/add` waited ~30s on the
embed queue, then the HTTP write timed out ("Remote end closed connection") —
kanban-sync failed every 15 min, chaba-kanban-brief failed, report producers
went stale fleet-wide.

## What ships now

- `EmbeddingWorker` has a provider circuit breaker
  (`MDDB_EMBEDDING_CIRCUIT_FAILURES=5`, `MDDB_EMBEDDING_CIRCUIT_COOLDOWN=30s`).
  While open: `Enqueue` drops immediately (no `QUEUE_WAIT`), the worker
  declines queued jobs without calling the provider, and mid-job retries stop
  early. Documents are still stored — only the vector is deferred.
- `POST /v1/add` and `POST /v1/add-batch` accept `skipEmbedding` /
  `skipEmbeddings` to bypass the embed queue entirely for writes that do not
  need vectors.
- `/v1/add` answers `X-MDDB-Embedding: deferred|skipped`; `/v1/add-batch`
  already reports `embeddingDropped`.
- `/health` gains `embedding` `{queueSize, queueDepth, dropped, circuitOpen,
  consecutiveFailures, circuitRetryAfterSeconds}`; `/v1/vector-stats`
  `queue.circuit` gains `{state, consecutiveFailures, retryAfterSeconds}`.

## Detect (before writes time out)

- `curl -s $MDDB/health | jq .embedding` — `circuitOpen: true`, `dropped`
  climbing, `queueDepth` near `queueSize` = provider outage in progress.
- `curl -s $MDDB/v1/vector-stats | jq .queue` — same signal with the
  `circuit.state` and `retryAfterSeconds`.
- Log lines: `embedding provider circuit open` (breaker tripped) and
  `embedding job dropped — ... reason=provider_circuit_open`.

## During an outage

- Nothing to do for write correctness — documents are stored; only vectors
  are deferred. Writes answer promptly instead of timing out.
- Callers that never need vectors can pass `skipEmbedding`/`skipEmbeddings`
  and skip the dropped-counter bookkeeping entirely.
- If vectors are more important than write latency for a deployment,
  `MDDB_EMBEDDING_CIRCUIT_FAILURES=0` disables the breaker (old behaviour:
  every write waits out `QUEUE_WAIT`).

## Post-recovery: reindex dropped vectors

The breaker closes itself on the first successful Embed after the cooldown —
no restart needed. Then repair the documents that were stored without a
vector:

```bash
# Per collection that was written to during the outage (mddb-cms: kanban cards,
# briefs, reports):
curl -s -X POST $MDDB/v1/vector-reindex \
  -H 'Content-Type: application/json' \
  -d '{"collection":"<collection>","force":false}'
```

`force:false` (default) re-embeds only docs whose stored vector is missing or
whose content hash changed — already-embedded docs are skipped, so this is
safe to run on every collection. Watch `embedded`/`skipped`/`failed` in the
response and `queue.circuit.state` in `/v1/vector-stats` afterwards.

To find which collections lost vectors: `/v1/vector-stats` →
`collections.<name>.embedded_documents < total_documents`.

## Caveats

- Vector-space correctness is untouched: same provider, same model, same
  dimensions — dropped docs simply have no vector until reindexed. Nothing
  silently re-embeds with a different model.
- `/health` stays 200 while the circuit is open on purpose: writes still work
  (docs are stored), so the node should not be pulled by a load balancer.
  Alert on `embedding.circuitOpen`/`embedding.dropped`, not the status code.
- `queue.dropped` is a process counter — it resets on restart. The durable
  signal is `embedded_documents < total_documents` in `/v1/vector-stats`.
