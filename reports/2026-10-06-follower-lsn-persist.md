# mddb follower — persist applied LSN across restarts

Date: 2026-10-06 · Card: mddb-follower-lsn-persist · Branch: follower-lsn-persist (rebased onto feat/vector-algorithms @ d77c6ae)

## Problem

Every follower restart came up with `lastAppliedLSN = 0` (in-memory only),
tripped the leader's below-oldest check, and re-pulled the full snapshot —
measured live on idc02: 4.27 GB → ~100 s stream + ~5 min verify
(2026-10-05 21:02), then 6.1 GB on the next restart.

## Decision: meta key inside the replicated DB, not a sidecar file

The applied LSN lives at `_mddb_replication / last_applied_lsn` inside the
follower's own BoltDB, written **in the same transaction** as the applied
entries:

- Atomic with the data — the marker can never run ahead of what is in the
  file; a crash mid-commit replays at most the uncommitted tail (idempotent).
- Costs no extra commit/fsync — the apply transaction was committing anyway,
  so the card's "not per-entry" concern is satisfied strictly.
- A sidecar `mddb.lsn` was rejected: it survives a manual database
  replacement and can end up ahead of the restored file, silently skipping
  entries forever.
- Travels with the file: delete the DB → marker gone → LSN 0 → snapshot, the
  correct fallback.

The bucket is follower-local. The leader never writes it; a snapshot swap
replaces the whole file, so `requestSnapshot` re-persists the snapshot LSN
right after the swap. `restoreFromBackup` reloads the marker so a manual
restore resumes from the restored file's position (older → replay, absent →
re-snapshot, both safe).

## Changes

- `replication_applier.go` — `replMetaBucket`/`replMetaLSNKey`,
  `putAppliedLSN` (tail of every apply tx), `loadPersistedLSN`,
  `PersistLSN`, `ReloadPersistedLSN`; `NewReplicationApplier` seeds
  `lastApplied` from the marker. Apply body extracted to `applyEntryTx`.
- `replication_client.go` — persists `snapshotLSN` after the swap.
- `restore_backup.go` — `ReloadPersistedLSN` after a restore on followers.
- `Dockerfile` — fully-qualified base images (podman hosts without
  unqualified-search registries, e.g. idc02, refuse short names).
- Tests: 9 new cases in `replication_applier_extra_test.go` covering write,
  restart reload, reopen durability, batch, checkpoint, `PersistLSN`,
  `ReloadPersistedLSN`, and bucket isolation.

## Base correction mid-flight

Work started on `binlog-retention` (0b46e57) — which lacks the
snapshot-loop fixes merged into `feat/vector-algorithms` (d77c6ae:
freelist-in-snapshot, `verifySnapshot` open-proof, progress logging,
`ad7708a` stream-test fix). An image built on the wrong base was briefly
deployed to idc02 (caught because its verify child took the slow full-check
path on the 6.1 GB snapshot); the branch was rebased onto
`feat/vector-algorithms`, image rebuilt (`localhost/mddb:2.15.4-lsn`,
`3c22910c755f`), and re-shipped. Lesson: build mddb deploys off
`feat/vector-algorithms`, the branch that produced the running images.

## Deployment

- Image built on idc02 per the card: `localhost/mddb:2.15.4-lsn`
  (`3c22910c755f`), `podman build -f services/mddbd/Dockerfile` from a
  rsynced `services/mddbd` context at `idc02:~/mddb-build-lsn/`.
- idc02 follower: quadlet `Image=` pinned, restarted.
- idc03 leader: image loaded via `tony-omen` hop (`ssh id_idc03`), quadlet
  `Image=` pinned, **not restarted** — leader restart = production outage,
  per the runbook it requires approval.

## Verification

- `cd services/mddbd && go test -count=1 .` — replication tests + 9 new
  marker tests pass.
- Live: idc02 restart logs — first restart on the new image re-snapshots
  once (no marker existed yet), writes the marker; a subsequent restart
  must log `replication applier resuming from persisted LSN` and stream
  instead of `requesting full snapshot`.
