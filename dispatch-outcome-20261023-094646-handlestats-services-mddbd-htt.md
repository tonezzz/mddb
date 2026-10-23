# dispatch outcome — mddb-stats-o1-counts

Card: mddb-stats-o1-counts · Runner: idc02 · Date: 2026-10-23
Branch: dispatch/20261023-094646-handlestats-services-mddbd-htt (commit 8174ae5, NOT pushed)

## What changed

`GET /v1/stats` no longer walks docs/rev/idxmeta (~1.6M keys on idc03) plus 57
per-collection checksum read txs on every request. The per-collection
counters + crc32 checksums are computed in ONE pass inside ONE read tx and
served from `statsCache` — a 60s-TTL, single-flight, stale-while-revalidate
cache (new file `services/mddbd/stats_cache.go`, Server field in `main.go`,
handler rewired in `http_handlers_meta.go`). Checksums fold into the same docs
walk and are byte-identical to `collectionChecksum()` (parity test included).
`?refresh=1` forces a synchronous recompute for audits needing right-now
numbers. Tenant filtering, collection attrs, DB size, index-queue stats remain
per-request (fresh). Option (a) counters-bucket was rejected: every write path
would need counter updates (drift risk) and it wouldn't fix checksums; can be
layered later.

Files: `services/mddbd/stats_cache.go` (new), `services/mddbd/stats_cache_test.go`
(new), `services/mddbd/http_handlers_meta.go`, `services/mddbd/main.go`,
`CHANGELOG.md`, `reports/2026-10-23-mddb-stats-snapshot-cache.md`.

## Result

- `GOWORK=off go build ./...` clean; `go vet` clean; gofmt clean.
- Full `mddb` package test suite passes (101s): all existing stats/checksum
  tests green, plus 5 new tests (checksum parity, cached-then-forced-refresh,
  stale-while-revalidate, concurrent cold single-flight, key parser edges).
- `-race` unavailable on this runner (no cgo/gcc) — concurrency covered by
  test + code review.
- Response schema unchanged; `/v1/checksum`, gRPC `Stats`, MCP untouched.

## Follow-ups / notes

- gRPC `Stats` (grpc_server.go) and the MCP stats path have the same
  per-request bucket walk — admin-only, out of card scope; they could reuse
  `collectCollectionStats`/`statsCache` in a follow-up.
- `stats.Uptime` field existed but was never populated — unchanged.

## Verify on idc03

After deploy: `curl /v1/stats` twice — second call within 60s should be
milliseconds; under load, concurrent `/metrics` + `/v1/vector-stats` should no
longer time out; `?refresh=1` still returns fresh numbers synchronously.
