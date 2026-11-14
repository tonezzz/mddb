# dispatch-outcome: mddb-binlog-upstream

Implemented the 5 replication snapshot-loop fixes from the card, on top of the
existing 4 binlog-retention commits. 8 new commits on
`dispatch/20261004-210641-upstream-binlog-retention-repl` — local only, nothing
pushed:

- `2bf9b3e` feat(replication): persist bbolt freelist in leader snapshots —
  `RequestSnapshot` `tx.CopyFile()`s to a temp file, flushes a freelist into it
  (bbolt writes one when a syncing handle opens an unsynced file), then streams
  the file. Followers never pay the O(every-page) freelist rebuild; `IsLast`
  marks the real final chunk.
- `60ac37c` feat(mddbd): default `MDDB_FREELIST_SYNC=true` on replication
  followers (explicit env overrides; leader/standalone unchanged).
- `497b982` fix(replication): open-only integrity proof for trusted snapshots —
  `swapDatabaseVerified(source, install, verify)`; the replication path proves
  the file opens (freelist walk + panic class still contained in the child)
  and skips `tx.Check()`. Restores keep the full check.
- `ed1ff11` feat(mddbd): progress logging during freelist/integrity walks —
  verify parent polls the child every 30s (`/proc` cpu + read_bytes vs size,
  elapsed); `withWalkProgress` wraps the freelist flush and post-swap reopen.
- `c0850f0` fix(replication): `/v1/replication/status` `healthy` reflects
  follower state — false at LSN 0, disconnected, or lagging beyond maxLag;
  reports `leader_addr`/`current_lsn`/`replication_lag_ms`.
- `44eeab9` docs(replication) + `3990db0` fix(mddbd): scrub check-mode env in
  the verify child.
- `ad7708a` test(replication): stream tests updated for the below-oldest →
  FailedPrecondition contract (pre-existing failure from `dc88ede` that hung
  the suite at the 10m timeout); added a contract test.

Result: the doom loop's cost drivers are removed — snapshots arrive with a
freelist (no O(db) rebuild on verify/reopen/restart), the verify is a bounded
open proof instead of an O(every-page) check, followers sync the freelist by
default, walks log progress instead of sitting silent for ~1h, and status
stops reporting healthy at LSN 0.

Verify: `cd services/mddbd && go test -count=1 .` (full suite) and
`go vet .` — clean. New tests cover each change (see `go test -run
'Freelist|VerifySnapshot|WalkProgress|FollowerHealthy|FollowerBelow'`).

Trail: `reports/2026-10-04-binlog-snapshot-loop-fixes.md`.
