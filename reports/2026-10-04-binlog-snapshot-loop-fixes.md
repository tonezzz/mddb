# mddb replication snapshot-loop fixes — decision record

Date: 2026-10-04 · Card: mddb-binlog-upstream · Branch: dispatch/20261004-210641-upstream-binlog-retention-repl

## Context

Follower snapshot restores on tony-dell doom-looped: ~4GB snapshot →
verify `tx.Check()` did not finish inside `MDDB_VERIFY_TIMEOUT=6h` → snapshot
discarded → follower still below binlog's oldest → requests another → leader
`keepLSN` pinned at `binlog_oldest_lsn` so the janitor trimmed nothing and the
binlog grew ~500MB/cycle. Root cost: every open of a NoFreelistSync image
rebuilds the freelist via `freepages → recursivelyCheckBucket` — O(every
page), ~1h+ under ~97% disk util — paid in the verify child, again on the
swap reopen, and again on every process restart.

## Decisions

1. **Leader persists the freelist into the snapshot.** `RequestSnapshot` now
   `tx.CopyFile()`s to a temp file (read tx held for a local copy, not the
   network stream) then opens the copy with freelist sync — bbolt flushes a
   freelist into any unsynced file on such an open. The walk is paid once on
   the leader, never on the follower. The open doubles as a proof the copy is
   usable; failure aborts the stream rather than feeding a bad file into a
   follower retry loop. `IsLast` now marks the actual final chunk.
2. **`MDDB_FREELIST_SYNC` defaults to true on followers** (explicit env still
   wins; leader/standalone unchanged). Followers are read-mostly; the per-commit
   cost is minor next to a page-walk rebuild on every restart.
3. **Snapshot verify = open proof, not `tx.Check()`.** `swapDatabase` takes a
   verifier; replication passes `verifySnapshot` (child opens with
   `PreLoadFreelist` — the same walk a real open does, so the panic class
   stays contained — and skips the O(every-page) invariant check). Restores
   keep the full check. Check-mode env vars are scrubbed from the child's
   inherited environment so a stray `MDDB_INTERNAL_VERIFY_OPEN_ONLY` cannot
   downgrade a restore.
4. **Progress logging.** Verify parent polls the child every 30s (elapsed +
   `/proc` cpu seconds + `read_bytes` vs file size — mmap page-ins count even
   though bbolt never calls `read()`); `withWalkProgress` wraps the leader's
   freelist flush and the post-swap reopen.
5. **`/v1/replication/status` reflects follower health.** `healthy` is false
   when the follower has no client, is not streaming, sits at LSN 0, or lags
   beyond `maxLag`; response gains `leader_addr`, `current_lsn` (applier),
   `replication_lag_ms`. `IsHealthy` drops time-since-last-entry (idle-but-
   synced is healthy) for connected + LSN>0 + lag.

## Also found

- `replication_stream_test.go` encoded the pre-`dc88ede` contract (streams
  `FromLsn:0`): two tests failed/hung (suite hit the 10m timeout). Updated to
  stream from the file's oldest LSN; added a test pinning the below-oldest →
  FailedPrecondition contract.

## Verification

`cd services/mddbd && go test -count=1 .` — full suite.
