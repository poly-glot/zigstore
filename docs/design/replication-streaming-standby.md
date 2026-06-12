# Streaming-Standby Replication — Design of Record

Postgres-style physical replication for `zigstore`: a leader streams its durable WAL to
followers that replay it into their own stores, with optional synchronous-commit quorum and
manual promotion. Tier 2b of the scaling roadmap; it also builds Tier 1's substrate
(monotonic LSN, WAL follow reader, leader streaming, follower apply).

## What the engine ships, and what it deliberately does not

The engine ships **mechanism**: the `Hub` (leader), the `Receiver` (follower), the
`CommitGate` (quorum), the WAL retain floor, and `read_only` + `promote()`/`demote()` +
status on the generated store. **Election and fencing stay with the consumer** — a
Kubernetes Lease (or any external coordinator) decides who leads. The old leader MUST be
fenced before a standby is promoted; there are no timeline ids, so the engine cannot detect
a resurrected leader on its own. The data-level case it can detect — a follower whose WAL is
ahead of the leader's — fails the handshake with `diverged` and parks the receiver.

## Decisions

### A separate replication port with blocking thread-per-follower I/O

Replication listens on its own port with one sender thread and one ack thread per follower.
Pushing the stream through the epoll data plane would have required deferred responses and a
new frame family in the client-facing protocol; followers number 2–5, so threads are the
simpler, equally correct tool. The epoll reactor and the client wire format are untouched —
the MINOR/additive protocol promise holds.

### Monotonic LSN across checkpoint truncation

`truncateAfterCheckpoint` used to reset the WAL sequence to 0, which made the sequence
useless as a replication LSN. It now preserves the sequence (the file is truncated; the
numbering continues), and `WalWriter.init` takes a `base_sequence` resumed from
`snapshot.meta`, so the LSN survives restarts even when the WAL file is empty.

Truncation also bumps an in-memory **truncation epoch** carried in `DurableBoundary`, so a
leader-side `FollowReader` detects "the file restarted under me" deterministically and
rescans from offset 0. (The earlier `boundary.offset < reader.offset` comparison was a
heuristic that an O_DIRECT writer could out-pace.) The epoch is process-local by design: a
leader restart recreates the hub and its readers with it.

### Connected-followers-only retain floor; lsn_too_old re-bootstrap

The hub holds `WalWriter.setRetainFloor` at the minimum durable ack of the *connected*
followers; `truncateAfterCheckpoint` fails with `error.WalRetainedByReplica` while any
connected follower has not acked the full WAL. There are **no persistent replication
slots** in v1: a dead replica releases the floor when its connection reaps, which avoids the
disk-full footgun of a forgotten slot pinning the WAL forever. The cost is explicit and
loud: a follower that falls behind a checkpoint truncation gets `lsn_too_old` at handshake
and parks in `needs_rebootstrap` — re-seed it with `fetchBaseBackup`, reopen the store, and
restart the receiver.

### Synchronous replication as a commit gate

`commit` already waits for local durability (`awaitDurable`). With `sync_standbys = n` and
`Hub.commitGate()` wired into the store via `setCommitGate`, it then also blocks until the
n-th highest follower durable ack covers the entry — the analog of Postgres
`synchronous_commit = on`. Acks carry both durable and applied LSNs, so `Hub.status` can
report apply lag separately. `Hub.stop` closes the gate and releases blocked commits with
`error.ReplicationStopped`: with RPO=0 semantics, an unacknowledged commit must fail rather
than silently degrade to asynchronous.

### The follower applies through the same seam as recovery

The `Receiver` appends each streamed entry to the replica's **own** WAL (so a promoted
standby is durable from its first second) and applies it through a consumer-supplied
`apply_entry` hook — the same shape `recover` takes. Only the receiver writes a replica's
WAL: `commit` fails with `error.ReadOnlyReplica` while the store is demoted, which is what
makes the receiver's `expected = getSequence() + 1` continuity check sound.

### Base backup over the same port; locked copy, unlocked transfer

Bootstrapping a replica (or recovering one parked in `needs_rebootstrap`) is one call:
`fetchBaseBackup` requests a base from the leader's replication port (same 48 B handshake
frame with `BACKUP_MAGIC` instead of `HANDSHAKE_MAGIC` — additive; an old leader replies
`rejected`), wipes any stale `wal.bin`, writes `store.dat` then `snapshot.meta`, and
returns the snapshot's WAL sequence. Reopening the store resumes the LSN from that
metadata, so `Receiver.start` streams exactly past the base.

Because pages mutate in place, an unlocked copy of `store.dat` could tear and WAL replay
cannot repair a torn page image. `forceBaseBackup` therefore copies `store.dat` to
`store.dat.base` while still holding the apply and drain locks (a local disk copy — the
short blocking window), then the hub streams the settled copy over the network with no
locks held and deletes it after. Backups are serialized by a hub-level mutex and served on
their own thread so a long transfer never stalls follower reconnects.

### Failure handling: transient reconnect, terminal park

Socket errors, CRC mismatches, and sequence gaps are transient: the receiver reconnects with
doubling backoff, and the handshake (`start_lsn` = the replica's own WAL sequence)
self-heals any gap or surfaces `lsn_too_old`. Three verdicts are terminal and end the
supervisor thread — `needs_rebootstrap`, `diverged`, `failed` (a local WAL or apply error
must never be acked past) — because each needs an operator or the consumer's control loop,
not a retry.

## Health and readiness (the Tier 0 op convention)

Ops are app-defined, so the engine ships the facts, not the op: `Store.healthStatus()`
returns `{ read_only, last_applied_lsn, durable_lsn }`, cheap enough for a probe path. The
convention for a consumer:

- expose a **ping/health op** (dmozdb convention: a high op number, e.g. 255) that returns
  `healthStatus()` — TCP connect alone is a liveness check; this is the readiness one;
- a **leader** is ready when it answers the op and `read_only` is false;
- a **replica** is ready when `read_only` is true and its apply lag is within budget:
  `Receiver.status().leader_durable_lsn - healthStatus().last_applied_lsn <= N` — the same
  numbers the generated TS router's read-your-writes fence consumes;
- `Hub.status` gives the leader-side view of every follower's durable/applied acks for
  dashboards and alerting.

## Promotion runbook (consumer-side; the engine ships only the mechanism)

The engine has no election, no fencing, and no timeline ids — `promote()` flips a flag.
The consumer's control loop (a Kubernetes Lease holder, an operator, a script) owns the
order of operations, and the order is what makes it safe:

1. **Fence the old leader first.** Make it unable to accept writes before anything else:
   delete/cordon its pod, cut it off with a NetworkPolicy, or — if it is still reachable —
   call `demote()` on it and stop its `Hub`. A paused-not-dead leader that wakes up later
   and keeps writing is the split-brain case nothing downstream can repair.
2. **Pick the most caught-up standby** (highest `Receiver.status().last_durable_lsn`; with
   `sync_standbys > 0` any acked standby is at or past every acknowledged commit).
3. On the chosen standby: `receiver.stop()`, then `store.promote()`, then `Hub.start` on
   it so the remaining standbys re-point and resume streaming from their own LSNs.
4. **Re-point traffic** (the writer Service selector / the TS router's leader transport).
5. **Re-join the old leader as a standby, never as a writer.** If it accepted any write the
   new leader never saw, its handshake fails `diverged` — wipe its `data_dir`, re-seed with
   `fetchBaseBackup`, and start a `Receiver`. The diverged verdict is the engine's last
   line of defense, not the fencing mechanism.

## Wire format (version 1)

Handshake (little-endian, exact sizes locked by comptime asserts):

- request, 48 B: `magic u32 = 0x5A524550` (stream) or `0x5A42414B` (base backup;
  `start_lsn` ignored), `version u32 = 1`, `start_lsn u64` ("I hold everything ≤ this"),
  `replica_name [32]u8` zero-padded.
- response, 16 B: `magic u32`, `status u8` (`accepted` / `lsn_too_old` / `diverged` /
  `rejected`), 3 pad bytes, `durable_lsn u64`.

Stream:

- leader → follower: `'E'` + the verbatim 24 B `wal.WalEntryHeader` + payload; `'H'` +
  `u64` leader durable LSN (heartbeat, default every 1 s when idle).
- follower → leader: `'A'` + `u64 durable_ack` + `u64 applied_ack` (group-committed: one ack
  per drained burst, and on every heartbeat).

Base backup (after an `accepted` response whose `durable_lsn` is the backup's WAL
sequence): `u64 len` + `snapshot.meta` bytes + `u32 crc32`, then `u64 len` + `store.dat`
bytes + `u32 crc32`; the leader closes when done. The CRC32 trailers are end-to-end checks
over each file's bytes — a mismatch fails the fetch with `BackupChecksumMismatch` rather
than seeding a silently corrupt replica. New trailing files would be additive.

Extensions follow the protocol rule: optional trailing bytes, never a forked message tag.

## Map

| Piece | Where |
|---|---|
| Monotonic LSN, retain floor, `DurableBoundary`, O_DIRECT reopen-hole fix | `src/wal.zig` |
| `FollowReader` (tail a live WAL below a durable boundary) | `src/wal_follow.zig` |
| `Hub`, `Receiver`, `CommitGate`, hosts, wire structs | `src/replication.zig` |
| `read_only` / `commit_gate` / `promote` / `demote` / `primaryHost` / `replicaHost`, snapshot-base LSN resume | `src/engine.zig` |
| Read-only reject + quorum wait in the write path | `src/commit.zig` |
