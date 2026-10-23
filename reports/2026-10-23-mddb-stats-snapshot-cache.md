# mddbd /v1/stats — snapshot cache (kanban: mddb-stats-o1-counts)

Date: 2026-10-23
Card: mddb-stats-o1-counts (runner idc02)
Evidence: chaba docs/ssot/jobs/infrastructure/2026-10-13-mddb-stats-timeout.yml

## Problem

`handleStats` (services/mddbd/http_handlers_meta.go) opened one bbolt read tx
and cursor-walked every key in `docs` (153k), `rev` (626k) and `idxmeta` (826k)
— `strings.Split(string(k))` per key — then ran `collectionChecksum()` per
collection (57 extra read txs, each prefix-walking that collection's docs with
crc32). On the 8.1GB idc03 DB: 0.9s warm to >60s under load; starved sibling
requests (concurrent /metrics and /v1/vector-stats timed out during the
2026-10-10 nightly audit, reproduced 2026-10-13); pinned a long read tx feeding
freelist/RSS growth that mddb-restart.timer exists to bound.

## Decision — options (b) + (c) from the card

Option (a) (counters bucket updated on write) was rejected for now: every write
path (HTTP add/update/delete, batch, bulk ingest, import, TTL reaper,
replication applier, restore) would have to maintain counters — drift risk is
high, and it does nothing for the checksums. Can be layered on later if 60s
staleness is ever a problem.

Implemented:

- `statsCache` on `Server` (stats_cache.go): 60s TTL snapshot of
  `map[string]*CollectionStats`, single-flight, stale-while-revalidate.
  - Fresh (< TTL) → serve cached.
  - Stale → serve stale, kick one background recompute.
  - Cold → synchronous compute (first request after startup only).
  - `GET /v1/stats?refresh=1` forces a synchronous recompute (piggybacks on an
    in-flight one) — escape hatch for audits needing right-now numbers.
- `collectCollectionStats()` walks the three buckets once in ONE read tx and
  folds the crc32 checksum into the docs pass (key + first 64 bytes of value,
  XOR fold — byte-identical to `collectionChecksum()`; parity locked by
  TestStatsCache_ChecksumParity). The 57 per-collection read txs are gone.
- Tenant filtering, collection type/description/icon/color enrichment,
  DatabaseSize and IndexQueue remain per-request — the cached part is only the
  expensive global map, so those fields stay fresh and tenants still see only
  their namespace.

## Effect

- Request cost: O(collections) map filter vs O(1.6M keys + 57 txs). Bounded:
  at most one bucket walk per 60s, never more than one at a time.
- Read-tx pinning: one tx per refresh max, independent of request rate.
- Response schema unchanged; `/v1/checksum`, gRPC `Stats`, MCP callers
  untouched (gRPC `Stats` in grpc_server.go has the same per-request walk —
  admin-only, left as-is; candidate to reuse the same snapshot later).

## Verify

```
cd services/mddbd
GOWORK=off go test -run 'TestStatsCache|TestStatsKeyCollection|TestHandleStats|TestSystemHandleStats|TestMainHandleStats|TestHandleChecksum' -v .
GOWORK=off go test .   # full package — passed 2026-10-23 (101s)
```

On idc03 after deploy: `time curl -s .../v1/stats` twice — first call may take
the walk, subsequent calls within 60s should be milliseconds; during a walk
under load, /metrics and /v1/vector-stats must no longer stall.
